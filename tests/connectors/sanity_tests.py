import unittest
import json
from unittest.mock import MagicMock, patch
from util.connectors import UrlInvoker
from util.auth_manager import TokenManager

class TestUrlInvoker(unittest.TestCase):
    
    @patch("util.connectors.get_success_responses")
    @patch("util.connectors.get_failed_responses_that_can_be_retried")
    def test_invoke_failed_responses_with_dict_body(self, mock_failed, mock_success):
        mock_token_manager = MagicMock(spec=TokenManager)
        mock_token_manager.get_valid_token_slot.return_value = {"token": "fake-token"}
        mock_token_manager.get_session.return_value = MagicMock()
        
        invoker = UrlInvoker(
            token_manager=mock_token_manager,
            batch_retry_count=1,
            batch_backoff=1,
            initial_delay=0,
            jitter=0
        )
        
        invoker.execute_batch_request = MagicMock(return_value={})
        mock_success.return_value = []
        mock_failed.return_value = [
            {
                "id": "1",
                "status": 500,
                "body": {"error": {"message": "Internal Server Error"}}
            }
        ]
        
        batch = [{"id": "1", "url": "/test"}]
        logger = MagicMock()
        
        # Execute the code
        result = invoker.invoke("https://graph.microsoft.com/v1.0", batch, logger)
        
        # NEW: We MUST assert that it logged the correct message. 
        # With the bug present, it logs a TypeError instead of the actual body!
        logger.assert_called_with('Consistent failures observed for the following: {"error": {"message": "Internal Server Error"}}')

if __name__ == "__main__":
    unittest.main()