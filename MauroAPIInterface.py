import requests
from urllib.parse import urljoin
import re

class MauroAPIInterface:
  def __init__(self, api_base_url = None, api_key = None):
    self.api_base_url = api_base_url
    self.api_key = api_key
    self.base_headers_for_get = {}
    self.base_headers_for_post = {}
    self.base_headers_for_put = {}

  @property
  def api_base_url(self):
    return self._api_base_url
  @api_base_url.setter
  def api_base_url(self, value):
    if (value is None or self.is_good_api_url(value)) :
      self._api_base_url = value
    else :
      raise ValueError("Given API URL appears to be bad.")

  def is_good_api_url(self, url):
    p = re.compile('^https?\:\/\/.*\/api\/?$')
    u = p.match(url)
    if u :
      return True
    else :
      return False

  @property
  def api_key(self):
    return self._api_key
  @api_key.setter
  def api_key(self, value):
    if (value is None or self.is_good_api_key(value)) :
      self._api_key = value
      self._api_key_header = {"apiKey" : value}
    else :
      raise ValueError("Given API key appears to be bad.")

  def is_good_api_key(self, key):
    p = re.compile('^[0-9a-f]{8}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{12}$', re.IGNORECASE)
    u = p.match(key)
    if u :
      return True
    else :
      return False
    

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

