from estimators.chat_estimator import ChatEstimator
from estimators.eo_group_mailbox_estimator import EOGroupMailBoxEstimator
from estimators.eo_in_place_archive_estimator import EOInPlaceArchiveEstimator
from estimators.eo_shared_mailbox_estimator import EOSharedMailBoxEstimator
from estimators.file_estimator import FileEstimator
from tests.files.mocks import MockUrlInvoker
from util.auth_manager import TokenManager
from util.connectors import UrlInvoker
from util.utils import ScanConfig
import json
import os

class EstimatorFactory:
  def __init__(
    self,
    config: ScanConfig,
    manager: TokenManager = None,
    one_token_per_app_manager = None,
    logger = None,
    stop_event = None,
    id_to_display_name = None
  ):
    self.manager = manager
    self.config = config
    self.logger = logger
    self.stop_event = stop_event
    self.id_to_display_name = id_to_display_name
    self.shared_mail_box_estimator = None
    self.group_mail_box_estimator = None
    self.in_place_archive_estimator = None
    self.files_estimator = None
    self.url_invoker = None
    self.chat_estimator = None
    self.mock_url_invoker = None
    self.one_token_per_app_manager = one_token_per_app_manager
    self.child_folder_url_invoker = None
  
  def isEmpty(self, data):
    return data is None or len(data) == 0

  def get_manager(self):
    if not self.manager:
      if self.isEmpty(self.config.client_ids) or self.isEmpty(self.config.client_secrets) or self.isEmpty(self.config.tenant_id):
        raise Exception("Missing credentials for tenant scan!!")
      self.manager = TokenManager(
        self.config.tenant_id,
        self.config.client_ids,
        self.config.client_secrets,
        self.config.concurrency,
        self.config.retries,
        self.config.backoff,
      )
    
    return self.manager

  def set_id_to_display_name(self, id_to_display_name):
    self.id_to_display_name = (
        id_to_display_name if id_to_display_name is not None else {}
    )

  def get_url_invoker(self, hard_reset=False):
    if self.url_invoker is None or hard_reset:
      self.url_invoker = UrlInvoker(
          self.manager, self.config.retries, self.config.backoff, 1, 0.5
      )

    return self.url_invoker
  
  def get_child_folder_url_invoker(self, hard_reset=False):
    if self.child_folder_url_invoker is None or hard_reset:
      if self.one_token_per_app_manager is None:
        raise Exception("One token per app manager not initialized!!")
      self.child_folder_url_invoker = UrlInvoker(
        self.one_token_per_app_manager, self.config.retries, self.config.backoff, 1, 0.5
      )

    return self.child_folder_url_invoker

  def get_mock_url_invoker(self, hard_reset=False, seed=None):
    if self.mock_url_invoker is None or hard_reset:
      data_path = "tests/files/test_data/state.json"
      if seed is not None:
        data_path = f"tests/files/test_data/state_{seed}.json"

      if not os.path.exists(data_path):
        raise FileNotFoundError(f"Test data not found at {data_path}. Please run data_state_creator.py first.")
          
      with open(data_path, "r") as f:
        test_data = json.load(f)

      self.mock_url_invoker = MockUrlInvoker(test_data)

    return self.mock_url_invoker

  def get_group_mailbox_estimator(self, hard_reset=False):
    if self.group_mail_box_estimator is None or hard_reset:
      if self.group_mail_box_estimator is not None:
        self.group_mail_box_estimator.shutdown()
      url_invoker = self.get_url_invoker()
      self.group_mail_box_estimator = EOGroupMailBoxEstimator(
          self.config,
          url_invoker,
          logger=self.logger,
          stop_event=self.stop_event,
      )
      self.group_mail_box_estimator.set_id_to_display_name_map(
          self.id_to_display_name
      )

    return self.group_mail_box_estimator

  def get_shared_mailbox_estimator(self, hard_reset=False):
    if self.shared_mail_box_estimator is None or hard_reset:
      if self.shared_mail_box_estimator is not None:
        self.shared_mail_box_estimator.shutdown()
      url_invoker = self.get_url_invoker()
      self.shared_mail_box_estimator = EOSharedMailBoxEstimator(
          self.config,
          url_invoker,
          logger=self.logger,
          stop_event=self.stop_event,
      )
      self.shared_mail_box_estimator.set_id_to_display_name_map(
          self.id_to_display_name
      )

    return self.shared_mail_box_estimator

  def get_in_place_archive_estimator(
      self, use_delta_api: bool = True, hard_reset: bool = False, use_for_eta_calc: bool = False
  ):
    if self.in_place_archive_estimator is None or hard_reset:
      if self.in_place_archive_estimator is not None:
        self.in_place_archive_estimator.shutdown()
      
      # Only initialize url invokers and token managers when we need to do full scan.
      if not use_for_eta_calc:
        url_invoker = self.get_url_invoker()
        child_folder_url_invoker = None if use_delta_api else self.get_child_folder_url_invoker()
        self.in_place_archive_estimator = EOInPlaceArchiveEstimator(
            self.config,
            url_invoker,
            child_folder_url_invoker,
            logger=self.logger,
            stop_event=self.stop_event,
            use_delta_api=use_delta_api,
        )
        self.in_place_archive_estimator.set_id_to_display_name_map(
          self.id_to_display_name
        )
      # If estimator is only needed for ETA estimation, we don't need to initialize the token managers.
      # Ideally we should not need this flow but it comes in handy in two cases:
      # 1. If IPA scan didn't take place then a dummy instance is needed
      # 2. If accidently the ui code shuts down the original estimator then a dummy instance is needed for ETA
      else:
        self.in_place_archive_estimator = EOInPlaceArchiveEstimator(
            self.config,
            None,
            None,
            logger=self.logger,
            stop_event=self.stop_event,
            use_delta_api=use_delta_api,
        )

    return self.in_place_archive_estimator

  def get_chat_estimator(self, hard_reset=False):
    if self.chat_estimator is None or hard_reset:
      if self.chat_estimator is not None:
        self.chat_estimator.shutdown()
      url_invoker = self.get_url_invoker()
      self.chat_estimator = ChatEstimator(
          self.config,
          url_invoker,
          logger=self.logger,
          stop_event=self.stop_event,
      )
      self.chat_estimator.set_id_to_display_name_map(self.id_to_display_name)
    return self.chat_estimator

  def get_files_estimator(self, progress_update_callback=lambda x: None, hard_reset=False, use_mocks=False, mock_seed=None):
    if self.files_estimator is None or hard_reset:
      if self.files_estimator is not None:
        self.files_estimator.shutdown()
      
      if not use_mocks:
        url_invoker = self.get_url_invoker()
      else:
        url_invoker = self.get_mock_url_invoker(hard_reset=hard_reset, seed=mock_seed)
      self.files_estimator = FileEstimator(
        self.config,
        url_invoker,
        logger=self.logger,
        stop_event=self.stop_event,
        progress_update_callback=progress_update_callback
      )
      self.files_estimator.set_id_to_display_name_map(self.id_to_display_name)
    
    return self.files_estimator 
