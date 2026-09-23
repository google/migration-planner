# Design Plan: Federated Identity Connector Provisioning

## Overview
Enable creation of a new Connector with `FederatedIdentity` authentication by copying connection details from a source connector and provisioning cloud resources (GCP Project, GCS Bucket) via DCP.

## 1. MACS Proto Changes ([connector.proto](http://google3/ccc/hosted/macs/proto/v1/connector.proto))
*   Add `FederatedIdentityAuthInfo` (or similar name) to `ConnectorAuthInfo.auth_info` oneof.
*   `FederatedIdentityAuthInfo` structure:
    *   `tenant_id` (String, Input): Copied from source.
    *   `client_id` (String, Input): Copied from source.
    *   `oidc_token_id` (String, Input): Required for federated auth.
    *   `gcp_project_name` (String, Output Only): Provisioned by DCP.
    *   `gcs_bucket` (String, Output Only): Provisioned by DCP.

### Code Snippet:
```protobuf
// In connector.proto

message ConnectorAuthInfo {
  oneof auth_info {
    // ... existing ...
    GALAuthInfo gal_auth_info = 1;
    WorkspaceDwdAuthInfo workspace_dwd_auth_info = 2;
    // ...
    FederatedIdentityAuthInfo federated_identity_auth_info = 6;
  }
}

message FederatedIdentityAuthInfo {
  // Input fields (Copied from source or provided by user)
  string tenant_id = 1 [(datapol.semantic_type) = ST_IDENTIFYING_ID];
  string client_id = 2 [(datapol.semantic_type) = ST_IDENTIFYING_ID];
  string oidc_token_id = 3 [(datapol.semantic_type) = ST_IDENTIFYING_ID];
  
  // Output only fields provisioned by DCP
  string gcp_project_name = 4 [
    (datapol.semantic_type) = ST_IDENTIFYING_ID,
    (validator.rule) = {
      predicate: "empty($) ?: gcp_project_name is output only and must not be specified"
    }
  ];
  
  string gcs_bucket = 5 [
    (datapol.semantic_type) = ST_IDENTIFYING_ID,
    (validator.rule) = {
      predicate: "empty($) ?: gcs_bucket is output only and must not be specified"
    }
  ];
}
```

## 2. Runway Backend (Migrate API) Changes
*   Update `CreateConnection` to handle the **"Generate Token"** flow.
*   **Logic Flow (Generate Token):**
    1.  Identify the trigger (e.g., a specific flag or source connection ID in request).
    2.  Fetch Source Connection details from MACS/Runway storage.
    3.  Extract relevant connection details (e.g., `tenant_id`, `client_id`).
    4.  Construct a new MACS `Connector` payload with `FederatedIdentityAuthInfo`, filling the copied fields.
    5.  Call MACS `CreateConnector`. The connector will be created in **DISABLED** state with reason **AUTHENTICATION_PENDING**.

*   Handle **"Verify Token"** flow via standard `EnableConnection` (or dedicated validation) API.
*   **Logic Flow (Verify Token):**
    1.  Trigger MACS `EnableConnector`.
    2.  MACS handles verification internally (see below).

### Code Snippet (Conceptual in Runway Handler):
// Likely placed in or near: [java/com/google/ccc/hosted/migration/tern/controlplane/runway/service/connection/actions/create/](http://google3/java/com/google/ccc/hosted/migration/tern/controlplane/runway/service/connection/actions/create/)
// Example pattern: [O365CreateConnectionRpcHandler.java](http://google3/java/com/google/ccc/hosted/migration/tern/controlplane/runway/service/connection/actions/create/O365CreateConnectionRpcHandler.java)

```java
// Conceptual logic in CreateConnectionRpcHandler or a helper

public Promise<Operation> createFederatedConnection(CreateConnectionRequest request) {
    String sourceConnectionId = request.getConnection().getAdditionalConfigMap().get("source_connection_id");
    
    if (sourceConnectionId != null) {
        // 1. Fetch Source Connector
        return macsClient.getConnector(sourceConnectionId)
            .thenChain(sourceConnector -> {
                // 2. Extract and Copy Details
                String tenantId = extractTenantId(sourceConnector);
                String clientId = extractClientId(sourceConnector);
                
                // 3. Build New Connector with Federated Auth
                Connector.Builder newConnectorBuilder = request.getConnector().toBuilder();
                newConnectorBuilder.setAuthInfo(
                    ConnectorAuthInfo.newBuilder()
                        .setFederatedIdentityAuthInfo(
                            FederatedIdentityAuthInfo.newBuilder()
                                .setTenantId(tenantId)
                                .setClientId(clientId)
                                .setOidcTokenId("user-provided-or-generated")
                                // gcp_project_name and gcs_bucket are empty, to be provisioned
                                .build()
                        )
                );
                
                // 4. Call MACS CreateConnector
                return macsClient.createConnector(newConnectorBuilder.build());
            });
    }
    // Standard creation flow...
}
```

## 3. MACS Backend Changes
*   Implement a new handler: `FederatedIdentityMultiTenantCreateConnectorHandler`.
*   **Logic Flow (CreateConnector):**
    1.  Persist Connector in initial **DISABLED** state with reason **AUTHENTICATION_PENDING**.
    2.  Call DCP (Data Connectors Platform) or relevant provisioning service to create:
        *   A new GCP Project (or fetch existing tenant project).
        *   A GCS Bucket within that project.
    3.  Populate the `gcp_project_name` and `gcs_bucket` fields in the `FederatedIdentityAuthInfo`.
    4.  Return the Connector (still DISABLED, but with metadata provisioned).

*   Update `EnableConnectorHandlerImpl` to handle **"Verify Token"** (EnableConnector API).
*   **Logic Flow (EnableConnector):**
    1.  In `processAuthentication`, add a case for `FEDERATED_IDENTITY_AUTH_INFO`.
    2.  Call Microsoft (via DCP or internal transport) to verify if the federated identity has been correctly added to the Azure App.
    3.  If verified, allow state transition to **ENABLED**.
    4.  If verification fails, throw an exception or keep in **DISABLED** state.

### Code Snippet (Conceptual MACS Handler):
// Likely placed in: [java/com/google/ccc/hosted/macs/service/connector/handler/createconnector/multitenant/](http://google3/java/com/google/ccc/hosted/macs/service/connector/handler/createconnector/multitenant/)
// Example pattern: [DwdMultiTenantCreateConnectorHandler.java](http://google3/java/com/google/ccc/hosted/macs/service/connector/handler/createconnector/multitenant/DwdMultiTenantCreateConnectorHandler.java)

```java
class FederatedIdentityMultiTenantCreateConnectorHandler implements MultiTenantCreateConnectorHandler {

  private final ManagedAppConnectorsDao managedAppConnectorsDao;
  private final DcpClient dcpClient; // Conceptual DCP Provisioning Client

  @Override
  public Promise<Operation> execute(Connector connector, String clientId) {
    // 1. Persist initial state (DISABLED, CREATION_IN_PROGRESS)
    return persistConnectorMetadata(generateImmutableConnectorInInitialState(connector))
        .thenChain(persistedConnector -> {
            
            // 2. Call DCP to provision resources
            String tenantId = persistedConnector.getAuthInfo().getFederatedIdentityAuthInfo().getTenantId();
            return dcpClient.provisionCloudResources(tenantId)
                .thenChain(provisionedResources -> {
                    
                    // 3. Update with provisioned resources
                    Connector.Builder updatedConnectorBuilder = persistedConnector.toBuilder();
                    updatedConnectorBuilder.getAuthInfoBuilder().getFederatedIdentityAuthInfoBuilder()
                        .setGcpProjectName(provisionedResources.getProjectName())
                        .setGcsBucket(provisionedResources.getBucketName());
                    
                    // Update state to ENABLED
                    updatedConnectorBuilder.setStateInfo(
                        ConnectorStateInfo.newBuilder().setState(ConnectorState.ENABLED));

                    return updateConnectorMetadata(updatedConnectorBuilder.build());
                });
        })
        .then(this::generateCompletedLro);
  }
}
```

## 4. UI / FEDS Changes
*   Add the control (checkbox/button) in the Data Import UI to trigger Federated Identity flow.
*   **"Generate Token" Button:**
    *   Triggers creation of the connector.
    *   Displays provisioned details (e.g. GCS bucket/GCP project info if needed by user for setup) and instructions on how to configure Federated Identity in Microsoft.
    *   Leaves UI in a "Pending Verification" state.
*   **"Verify Token" Button:**
    *   Triggers Runway `EnableConnection` (which calls MACS `EnableConnector`).
    *   Verifies with Microsoft if the setup is correct.
    *   If successful, transitions UI to "Connected/Ready" state.

---
## Feasibility Assessment
*   **Copying Connection Details:** Highly feasible. Runway Backend can read existing connectors.
*   **DCP Provisioning:** Feasible assuming DCP has APIs for provisioning projects/buckets. MACS already handles service account creation in similar handlers, so adding GCP/GCS provisioning follows existing patterns.
*   **Storing Outputs in AuthInfo:** Follows existing patterns (e.g., `service_account_id` in `WorkspaceDwdAuthInfo` is output only).
*   **Two-Step Verification:** Feasible and standard pattern in MACS (e.g., `AUTHENTICATION_PENDING` state). Verifying with Microsoft requires an outbound call to Azure AD to validate the federated credential setup.

---

# CL Plan: Federated Identity Connector Provisioning

This plan outlines the steps to implement the Federated Identity Connector Provisioning flow, moving from Proto changes to Backend implementation and finally UI updates. This plan is scoped for **Runway V2**.

---

## CL 1: MACS Proto Changes

**Goal:** Define the data structure for Federated Identity Authentication in MACS.

*   **Target File:** [`ccc/hosted/macs/proto/v1/connector.proto`](http://google3/ccc/hosted/macs/proto/v1/connector.proto)
*   **Changes:**
    *   Add `FederatedIdentityAuthInfo` message.
    *   Add `federated_identity_auth_info` to `ConnectorAuthInfo.auth_info` oneof.

### Proposed Proto Structure:
```protobuf
message ConnectorAuthInfo {
  oneof auth_info {
    // ... existing ...
    GALAuthInfo gal_auth_info = 1;
    WorkspaceDwdAuthInfo workspace_dwd_auth_info = 2;
    // ...
    FederatedIdentityAuthInfo federated_identity_auth_info = 6;
  }
}

message FederatedIdentityAuthInfo {
  // Input fields (Copied from source by Runway layer)
  string tenant_id = 1 [(datapol.semantic_type) = ST_IDENTIFYING_ID];
  string client_id = 2 [(datapol.semantic_type) = ST_IDENTIFYING_ID];
  
  // Output only fields provisioned/generated by DCP
  string oidc_token_id = 3 [
    (datapol.semantic_type) = ST_IDENTIFYING_ID,
    (validator.rule) = {
      predicate: "empty($) ?: oidc_token_id is output only and must not be specified"
    }
  ];
  
  string gcp_project_name = 4 [
    (datapol.semantic_type) = ST_IDENTIFYING_ID,
    (validator.rule) = {
      predicate: "empty($) ?: gcp_project_name is output only and must not be specified"
    }
  ];
  
  string gcs_bucket = 5 [
    (datapol.semantic_type) = ST_IDENTIFYING_ID,
    (validator.rule) = {
      predicate: "empty($) ?: gcs_bucket is output only and must not be specified"
    }
  ];
}
```

---

## CL 2: MACS Backend - New Handler Implementation

**Goal:** Implement the logic to create a Federated Identity Connector and initiate DCP provisioning.

*   **Target Directory:** [`java/com/google/ccc/hosted/macs/service/connector/handler/createconnector/multitenant/`](http://google3/java/com/google/ccc/hosted/macs/service/connector/handler/createconnector/multitenant/)
*   **Action:** Create `FederatedIdentityMultiTenantCreateConnectorHandler.java`.
*   **Logic:**
    1.  Persist Connector in **DISABLED** state with reason **AUTHENTICATION_PENDING**.
    2.  Call DCP Provisioning Client (to be confirmed/integrated) to:
        *   Create/Fetch GCP Project.
        *   Create GCS Bucket.
        *   Generate OIDC Token ID.
    3.  Populate `oidc_token_id`, `gcp_project_name`, and `gcs_bucket` in `FederatedIdentityAuthInfo`.
    4.  Update Connector metadata in DB.
    5.  Return the Connector.
*   **Target File:** [`java/com/google/ccc/hosted/macs/service/connector/handler/createconnector/CreateConnectorHandlerModule.java`](http://google3/java/com/google/ccc/hosted/macs/service/connector/handler/createconnector/CreateConnectorHandlerModule.java)
*   **Action:** Bind the new handler to the corresponding auth type.

---

## CL 3: Runway Backend - Create Connection Logic (V2)

**Goal:** Update Runway layer to handle Federated Identity creation by copying from source.

*   **Target Area:** Runway V2 Connection Creation path (e.g., in or near [`CreateConnectionRpcHandlerV2.java`](http://google3/java/com/google/ccc/hosted/migration/tern/controlplane/runway/service/connection/actions/create/CreateConnectionRpcHandlerV2.java) or adapters).
*   **Logic:**
    1.  If the request triggers this flow (e.g., via a specific flag in request or `additional_config` indicating "Generate Token" for Federated Identity):
    2.  Scan all Connectors for the customer.
    3.  Find a Connector that is **ENABLED** and has **`GalAuthInfo`** populated.
    4.  Extract `tenant_id` and `client_id` from its `GalConfigInfo`.
    5.  Construct the MACS `CreateConnectorRequest` setting these fields in `FederatedIdentityAuthInfo`.
    6.  Call MACS `CreateConnector`.

---

## CL 4: MACS Backend - Enable/Verify Logic

**Goal:** Implement verification with Microsoft when enabling the connector.

*   **Target File:** [`java/com/google/ccc/hosted/macs/service/connector/handler/enableconnector/EnableConnectorHandlerImpl.java`](http://google3/java/com/google/ccc/hosted/macs/service/connector/handler/enableconnector/EnableConnectorHandlerImpl.java)
*   **Action:** Update `processAuthentication` method.
*   **Logic:**
    1.  Add a case for `FEDERATED_IDENTITY_AUTH_INFO`.
    2.  Call Microsoft (Azure AD) via appropriate transport/client to verify if the federated credential has been correctly added to the application (similar to client secret verification).
    3.  If verified, return success to allow transition to **ENABLED** state.
    4.  If verification fails, throw appropriate exception/error.

---

## CL 5: UI / FEDS Changes

**Goal:** Provide user controls for generating and verifying tokens.

*   **Target Area:** Data Import / Migration Setup UI.
*   **Changes:**
    *   Implement **"Generate Token"** button:
        *   Calls Runway API to create the connector (CL 3).
        *   Handles response to show provisioned details (OIDC Token ID, etc.) to the user for Microsoft setup.
    *   Implement **"Verify Token"** button:
        *   Calls Runway `EnableConnection` (which triggers CL 4).
        *   Shows success/failure feedback (similar to client secret validation).
    *   Manage UI states (e.g., Disabled/Pending -> Verified/Ready).


