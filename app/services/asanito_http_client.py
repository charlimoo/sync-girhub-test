# app/services/asanito_http_client.py
import requests
import logging
import json
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

class AsanitoHttpClient:
    """
    A robust, synchronous HTTP client for making authenticated calls to the Asanito API.

    This client handles:
    - Dynamic URL construction with path and query parameters.
    - Automatic re-authentication on 401 Unauthorized errors with a single retry.
    - Detailed logging of requests and responses for easy debugging.
    - Graceful handling of network errors and non-JSON responses.
    - A standardized response format.
    """

    def __init__(self, asanito_service, job_id="DataSyncJob"):
        """
        Initializes the client with an authenticated AsanitoService instance.

        Args:
            asanito_service: An instance of the AsanitoService.
            job_id (str): The identifier of the job using this client, for logging purposes.
        """
        self.service = asanito_service
        self.log_prefix = f"AsanitoClient ({job_id})"

    def request(self, method, endpoint_template, path_params=None, query_params=None, body_payload=None, timeout=30.0):
        """
        Makes an authenticated request to the Asanito API.

        Args:
            method (str): The HTTP method (e.g., 'GET', 'POST', 'PUT').
            endpoint_template (str): The API endpoint, with placeholders for path params (e.g., '/users/{user_id}').
            path_params (dict, optional): Dictionary of parameters to format into the endpoint.
            query_params (dict, optional): Dictionary of query string parameters.
            body_payload (dict, optional): The JSON payload for POST/PUT requests.
            timeout (float, optional): Request timeout in seconds.

        Returns:
            dict: A standardized dictionary containing 'status_code' and either 'data' or 'error'.
        """
        http_method = method.upper()
        
        try:
            # 1. Construct the full URL
            request_url = self._build_url(endpoint_template, path_params, query_params)

            # 2. Make the request with retry logic
            return self._make_request_with_retry(http_method, request_url, body_payload, timeout)

        except ValueError as ve:
            logger.error(f"{self.log_prefix}: Value error during request setup: {ve}")
            return {"error": str(ve), "status_code": 400}
        except Exception as e:
            logger.exception(f"{self.log_prefix}: Unexpected internal error in HTTP client", exc_info=True)
            return {"error": f"An unexpected internal error occurred: {e}", "status_code": 500}

    def _build_url(self, endpoint_template, path_params, query_params):
        """Builds the complete request URL from templates and parameters."""
        final_endpoint = endpoint_template
        if path_params:
            try:
                final_endpoint = endpoint_template.format(**path_params)
            except KeyError as e:
                raise ValueError(f"Missing path parameter {e} for endpoint '{endpoint_template}'")

        base_url = self.service.base_url.rstrip('/')
        full_url = f"{base_url}{final_endpoint}"

        if query_params:
            # Filter out None values and format booleans correctly
            filtered_params = {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in query_params.items() if v is not None}
            if filtered_params:
                query_string = urlencode(filtered_params)
                full_url += f"?{query_string}"
        
        return full_url

    def _make_request_with_retry(self, method, url, payload, timeout):
        """Handles the core request logic, including a single retry on 401."""
        try:
            # Initial attempt
            return self._execute_request(method, url, payload, timeout)
        
        except requests.exceptions.HTTPError as http_err:
            # Check for 401 Unauthorized to trigger re-authentication
            if http_err.response.status_code == 401:
                logger.warning(f"{self.log_prefix}: Received 401 Unauthorized. Attempting re-authentication and retry...")
                try:
                    # This call refreshes the token in the shared service instance
                    self.service._authenticate()
                    logger.info(f"{self.log_prefix}: Re-authentication successful. Retrying the request.")
                    # Second and final attempt
                    return self._execute_request(method, url, payload, timeout, is_retry=True)
                
                except Exception as auth_err:
                    logger.error(f"{self.log_prefix}: Re-authentication or retry failed: {auth_err}", exc_info=True)
                    # Return the error from the re-auth/retry attempt
                    return self._format_error_response(auth_err)

            # For any other HTTP error, format and return it
            return self._format_error_response(http_err)

        except requests.exceptions.RequestException as req_err:
            # For network errors (timeout, connection error, etc.)
            return self._format_error_response(req_err)

    def _execute_request(self, method, url, payload, timeout, is_retry=False):
        """Executes a single HTTP request and processes the response."""
        retry_prefix = "(Retry) " if is_retry else ""
        self._log_request(method, url, payload)

        headers = self.service._get_authenticated_headers()
        
        response = requests.request(
            method=method,
            url=url,
            json=payload,
            headers=headers,
            timeout=timeout
        )

        self._log_response(response, retry_prefix)
        response.raise_for_status()  # Raise HTTPError for 4xx/5xx responses

        # Success case: format and return the data
        try:
            data = response.json()
        except json.JSONDecodeError:
            # Handle cases with success status but non-JSON body (e.g., 204 No Content)
            data = response.text if response.text else "Operation successful with no content."
            
        return {"data": data, "status_code": response.status_code}
    
    def _log_request(self, method, url, payload):
        """Logs outgoing request details for debugging."""
        logger.info(f"{self.log_prefix}: Request -> {method} {url}")
        if payload:
            try:
                # Log payload, ensuring it's JSON serializable for clean printing
                logger.debug(f"{self.log_prefix}: Body -> {json.dumps(payload, indent=2, ensure_ascii=False)}")
            except TypeError:
                logger.debug(f"{self.log_prefix}: Body -> {payload} (Not JSON serializable for logging)")

    def _log_response(self, response, retry_prefix=""):
        """Logs incoming response details."""
        logger.info(f"{self.log_prefix}: {retry_prefix}Response <- Status {response.status_code} {response.reason}")
        # Log body only if it's not successful or the content is small
        if response.status_code >= 400 or (response.text and len(response.text) < 1000):
            logger.debug(f"{self.log_prefix}: Response Body -> {response.text[:1000]}")
            
    def _format_error_response(self, error):
        """Creates a standardized dictionary from an exception."""
        if isinstance(error, requests.exceptions.HTTPError):
            status_code = error.response.status_code
            try:
                # Try to get a meaningful error message from the JSON response
                error_json = error.response.json()
                message = error_json.get("message", error_json.get("error", "An HTTP error occurred."))
            except json.JSONDecodeError:
                message = error.response.text[:200] or "An HTTP error occurred with no response body."
            logger.error(f"{self.log_prefix}: HTTP {status_code} error: {message}")
            return {"error": message, "status_code": status_code}
        
        elif isinstance(error, requests.exceptions.Timeout):
            logger.error(f"{self.log_prefix}: Request timed out.")
            return {"error": "The request timed out.", "status_code": 504}
            
        elif isinstance(error, requests.exceptions.ConnectionError):
            logger.error(f"{self.log_prefix}: A connection error occurred.")
            return {"error": "A network connection error occurred.", "status_code": 503}

        else: # Generic catch-all
            logger.error(f"{self.log_prefix}: An unexpected error occurred: {error}", exc_info=True)
            return {"error": "An unexpected error occurred.", "status_code": 500}