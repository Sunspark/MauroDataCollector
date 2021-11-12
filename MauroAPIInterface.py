import requests
from urllib.parse import urljoin

class MauroAPIInterface:
  def __init__(self, api_base_url = None, api_key = None):
    self.api_key = api_key
    self.api_base_url = api_base_url
    self.base_headers_for_get = {}
    self.base_headers_for_post = {}
    self.base_headers_for_put = {}

  @property
  def api_key(self):
    return self._api_key
  @api_key.setter
  def api_key(self, value):
    self._api_key = value
    self._api_key_header = {"apiKey" : value}

  def get_headers_for_get(self):
    return self.base_headers_for_get | self._api_key_header

  def get_api_url(self, endpoint_url):
    return urljoin(self.api_base_url.rstrip("/") + "/", endpoint_url.lstrip("/"))
    
  def call(self, endpoint_url, call_method):
    if (call_method == 'GET'):
      return requests.get(self.get_api_url(endpoint_url), headers = self.get_headers_for_get())
    elif (call_method == 'POST'):
      raise FutureWarning("POST not yet implemented")
    elif (call_method == 'PUT'):
      raise FutureWarning("PUT not yet implemented")
    elif (call_method == 'DELETE'):
      raise FutureWarning("DELETE not yet implemented")
    else:
      raise ValueError("Unknown call_method (" + str(call_method) + ") passed to MauroAPIInterface.call.")

