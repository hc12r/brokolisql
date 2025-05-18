import pandas as pd
import requests
import json
from jsonpath_ng import parse
from brokolisql.exceptions.base import APIConnectionError, APIResponseError

class APIConnector:
    """Class for connecting to and reading data from REST APIs."""
    
    def __init__(self, url, method='GET', headers=None, auth=None, data=None, path=None):
        """
        Initialize API connector with request parameters.
        
        Args:
            url (str): API endpoint URL
            method (str): HTTP method (GET or POST)
            headers (str): JSON-formatted headers
            auth (str): Basic auth credentials in format "username:password"
            data (str): JSON-formatted request payload
            path (str): JSONPath expression to extract data from response
        """
        self.url = url
        self.method = method.upper()
        self.headers = self._parse_json(headers) if headers else {}
        self.auth = self._parse_auth(auth)
        self.data = self._parse_json(data) if data else None
        self.path = path
        
        if self.method not in ['GET', 'POST']:
            raise ValueError(f"Unsupported HTTP method: {method}. Supported methods are GET and POST.")
    
    def _parse_json(self, json_str):
        """Parse a JSON string into a Python object."""
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {str(e)}")
    
    def _parse_auth(self, auth_str):
        """Parse authentication string into a tuple."""
        if not auth_str:
            return None
        
        parts = auth_str.split(':', 1)
        if len(parts) != 2:
            raise ValueError("Auth should be in format 'username:password'")
        
        return tuple(parts)
    
    def fetch_data(self):
        """
        Fetch data from the API and return as a pandas DataFrame.
        
        Returns:
            pandas.DataFrame: DataFrame containing the API response data
        """
        try:
            # Set up the request
            request_kwargs = {
                'headers': self.headers,
                'auth': self.auth
            }
            
            # Make the request
            if self.method == 'GET':
                response = requests.get(self.url, **request_kwargs)
            else:  # POST
                request_kwargs['json'] = self.data
                response = requests.post(self.url, **request_kwargs)
            
            # Check if request was successful
            response.raise_for_status()
            
            # Parse the response
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                raise APIResponseError(
                    "Failed to parse API response as JSON",
                    "The API did not return valid JSON data."
                )
            
            # Extract data using JSONPath if provided
            if self.path:
                try:
                    jsonpath_expr = parse(self.path)
                    matches = jsonpath_expr.find(response_data)
                    
                    if not matches:
                        raise APIResponseError(
                            f"JSONPath '{self.path}' did not match any data",
                            "Check your JSONPath expression and API response structure."
                        )
                    
                    # Extract the values from the matches
                    data = [match.value for match in matches]
                    
                    # If we got a list of values that aren't dictionaries, transform them
                    if data and not isinstance(data[0], dict):
                        data = [{"value": item} for item in data]
                    
                    # If we got just one dictionary, wrap it in a list
                    if data and isinstance(data, dict):
                        data = [data]
                        
                except Exception as e:
                    raise APIResponseError(
                        f"Error applying JSONPath: {str(e)}",
                        "Check that your JSONPath expression is valid."
                    )
            else:
                # Use the entire response
                if isinstance(response_data, dict):
                    # If root is a dict, wrap it in a list for DataFrame conversion
                    data = [response_data]
                elif isinstance(response_data, list):
                    # If root is a list, use it directly
                    data = response_data
                else:
                    # For primitive types, create a simple structure
                    data = [{"value": response_data}]
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # If DataFrame is empty, raise an error
            if df.empty:
                raise APIResponseError(
                    "No data was returned from the API",
                    "Check your API endpoint and parameters."
                )
                
            return df
                
        except requests.exceptions.RequestException as e:
            raise APIConnectionError(
                f"Failed to connect to API: {str(e)}",
                "Check your URL, network connection, and API availability."
            )