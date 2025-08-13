# start of app/services/asanito_service.py
# app/services/asanito_service.py
import requests
import logging
from flask import current_app

logger = logging.getLogger(__name__)

class AsanitoService:
    def __init__(self):
        """
        Initializes the Asanito Service with configuration from the Flask app.
        """
        self.base_url = current_app.config['ASANITO_BASE_URL']
        self.mobile = current_app.config['ASANITO_MOBILE']
        self.password = current_app.config['ASANITO_PASSWORD']
        self.customer_id = current_app.config['ASANITO_CUSTOMER_ID']
        
        self.auth_headers = None
        # --- FIX: Rename internal attribute to avoid naming conflict with the property ---
        self._owner_user_id = None

    @property
    def owner_user_id(self):
        """
        A property that ensures authentication is performed before returning the user ID.
        This enables lazy authentication on first access.
        """
        if not self._owner_user_id:
            self._authenticate()
        return self._owner_user_id

    def _authenticate(self):
        """
        Performs authentication using the LoginWithPhoneNumber method.
        """
        if not all([self.base_url, self.mobile, self.password, self.customer_id]):
            raise ValueError("Asanito configuration (URL, Mobile, Password, CustomerID) is missing.")

        logger.info("Attempting to authenticate with Asanito using phone number...")
        
        try:
            login_url = f"{self.base_url}/api/auth/Account/LoginWithPhoneNumber"
            login_payload = {
                "mobile": self.mobile,
                "password": self.password,
                "customerId": self.customer_id
            }
            
            logger.debug(f"Asanito auth request to {login_url} with payload: {login_payload}")
            login_response = requests.post(login_url, json=login_payload, timeout=15)
            login_response.raise_for_status()
            
            auth_data = login_response.json()
            access_token = auth_data.get('access_token')
            if not access_token:
                raise ValueError("'access_token' not found in login response.")

            logger.info("Step 1/2: Successfully obtained access token.")

            headers = {
                "accept": "application/json, text/plain, */*",
                "authorization": f"Bearer {access_token}",
                "content-type": "application/json",
            }

            user_info_url = f"{self.base_url}/api/asanito/User/getUserByToken"
            user_info_response = requests.get(user_info_url, headers=headers, timeout=15)
            user_info_response.raise_for_status()
            
            user_data = user_info_response.json()
            # --- FIX: Set the internal attribute ---
            self._owner_user_id = user_data.get("id")
            if not self._owner_user_id:
                raise ValueError("'id' not found in user data response.")
            
            self.auth_headers = headers
            logger.info(f"Step 2/2: Successfully authenticated and fetched user ID: {self._owner_user_id}")

        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred during Asanito authentication: {e}")
            self.auth_headers = None
            self._owner_user_id = None
            raise
        except ValueError as e:
            logger.error(f"A value error occurred during Asanito authentication: {e}")
            self.auth_headers = None
            self._owner_user_id = None
            raise

    def _get_authenticated_headers(self):
        """
        Ensures that the service is authenticated and returns the required headers for API calls.
        If not authenticated, it triggers the _authenticate method.
        """
        if not self.auth_headers:
            self._authenticate()
        return self.auth_headers

    # The rest of the file (add_or_update_contact) remains unchanged.
    def add_or_update_contact(self, contact_data):
        """
        Sends data to an Asanito endpoint to create/update a contact.
        This method now uses the more complex authentication flow.
        """
        try:
            headers = self._get_authenticated_headers()
            
            endpoint = f"{self.base_url}/api/asanito/Contacts" # Example endpoint
            external_id = contact_data['external_id']
            
            logger.info(f"Syncing contact with external ID: {external_id}")
            
            response = requests.put(f"{endpoint}/{external_id}", json=contact_data, headers=headers, timeout=20)
            
            response.raise_for_status()
            
            logger.info(f"Successfully synced contact {external_id}.")
            return response.json()
        
        except (requests.exceptions.RequestException, ValueError) as e:
            logger.error(f"Failed to sync contact {contact_data.get('external_id')}: {e}")
            raise
# end of app/services/asanito_service.py
# end of app/services/asanito_service.py