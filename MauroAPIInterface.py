import requests
from urllib.parse import urljoin
import re
import json

class MauroAPIInterface:
  def __init__(self, logger, api_base_url = None, api_key = None):
    self.logger = logger
    self.api_base_url = api_base_url
    self.api_key = api_key
    self.base_headers_for_get = {}
    self.base_headers_for_post = {}
    self.base_headers_for_put = {
      "Content-Type": "application/json",
      "Content-Length": str(1)
    }

    # from: {{base_url}}/path/prefixMappings, Mauro version: 4.10.0
    self.path_prefix_mappings = {
        "ann": "Annotation",
        "api": "ApiProperty",
        "auth": "Authority",
        "cl": "Classifier",
        "cs": "CodeSet",
        "cu": "CatalogueUser",
        "dc": "DataClass",
        "dcc": "DataClassComponent",
        "de": "DataElement",
        "dec": "DataElementComponent",
        "df": "DataFlow",
        "dm": "DataModel",
        "dt": "EnumerationType",
        "ed": "Edit",
        "ev": "EnumerationValue",
        "fo": "Folder",
        "gr": "GroupRole",
        "md": "Metadata",
        "rde": "ReferenceDataElement",
        "rdm": "ReferenceDataModel",
        "rdt": "ReferenceEnumerationType",
        "rdv": "ReferenceDataValue",
        "rev": "ReferenceEnumerationValue",
        "rf": "ReferenceFile",
        "rr": "RuleRepresentation",
        "rsm": "ReferenceSummaryMetadata",
        "rsmr": "ReferenceSummaryMetadataReport",
        "ru": "Rule",
        "sl": "SemanticLink",
        "sm": "SummaryMetadata",
        "smr": "SummaryMetadataReport",
        "te": "Terminology",
        "tm": "Term",
        "tr": "TermRelationship",
        "trt": "TermRelationshipType",
        "ug": "UserGroup",
        "uif": "UserImageFile",
        "vf": "VersionedFolder",
        "vl": "VersionLink"
    }    

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

  def get_headers_for_put(self, call_body):
    self.base_headers_for_put['Content-Length'] = str(len(call_body))
    self.logger.debug(self.base_headers_for_put)
    return self.base_headers_for_put | self._api_key_header    

  def get_api_url(self, endpoint_url):
    return urljoin(self.api_base_url.rstrip("/") + "/", endpoint_url.lstrip("/"))
    
  # requests.utils.quote('test+user@gmail.com')
  # It seems that requests quotes the URL internally.
  def call(self, endpoint_url, call_method, call_body = None):
    if (call_method == 'GET'):
      return requests.get(self.get_api_url(endpoint_url), headers = self.get_headers_for_get(), timeout=30)
    elif (call_method == 'POST'):
      raise FutureWarning("POST not yet implemented")
    elif (call_method == 'PUT'):
      return requests.put(self.get_api_url(endpoint_url), data=call_body, headers = self.get_headers_for_put(call_body))
    elif (call_method == 'DELETE'):
      raise FutureWarning("DELETE not yet implemented")
    else:
      raise ValueError("Unknown call_method (" + str(call_method) + ") passed to MauroAPIInterface.call.")

  # Takes an entity string suitable for the 'path' endpoint and makes a dict of it
  # Returns a dictionary :
  #   'entity_type' : Short string describing the entity type
  #   'entity_name' : The entity name
  def _split_to_entity_dict(self, entity_string) :
    entity_list = entity_string.split(':')
    return {
      'entity_type' : self.path_prefix_mappings[entity_list[0]],
      'entity_name' : entity_list[1]
    }
  
  # Takes a constructed path to an entity, suitable for the 'path' endpoint of Mauro.
  # This should either be in the form:
  #   'dm:maurodatamapper|dc:core|dc:annotation|de:last_updated' or
  #   'dm%3Amaurodatamapper%7Cdc%3Aapi_property'
  # Returns a dictionary :
  #   'status_code' : The numeric status of the http response
  #   'url_found' : True || False - was an appropriate URL found. Note that the API can return paths that don't match the incoming path.
  #   'model_finalised' : True || False - Is the current head of the data model 'finalised'.
  #   'id_based_url' : A string with the ID-based path to the entitiy
  #   'reason' : the response 'reason'
  #   'full_text' : the full response text
  def find_id_based_url_by_path(self, path_to_entity):
    #api_url = api_base_url + '/dataModels/path/dm%3Amaurodatamapper%7Cdc%3Aapi_property' # << definitely exists, GET (although the fact it exists is an error)
    #api_url = api_base_url + '/dataModels/path/dm%3AFISH' # << definitely NOT exists
    #api_url = api_base_url + requests.utils.quote('/dataModels/path/dm:maurodatamapper|dc:core|dc:annotation|de:last_updated') # << works and exists.

    api_endpoint = '/dataModels/path/'
    api_endpoint_path = api_endpoint + path_to_entity.lstrip("/")
    http_method = 'GET'
    self.logger.debug("Attempting call to endpoint: " + str(api_endpoint_path))
    self.logger.debug("With http method: " + str(http_method))
    r = self.call(api_endpoint_path, http_method)

    return_dict = {
      'status_code' : r.status_code,
      'reason' : r.reason,
      'full_text' : r.text,
      'url_found' : False,
      'model_finalised' : False,
      'id_based_url' : None
    }

    self.logger.debug("Response status code: " + str(r.status_code))
    self.logger.debug("Response text: " + r.text)

    if r.status_code != 200 :
      self.logger.debug("HTTP response bad: " + str(r.status_code))
      return return_dict
    else :
      # compare breadcrumbs to incoming path

      # de-construct incoming path
      clean_path_to_entity = path_to_entity.replace('%3A',':').replace('%7C','|')
      path_split = clean_path_to_entity.split('|')
      path_dict_list = list( self._split_to_entity_dict(x) for x in path_split)
      self.logger.debug("path list: ")
      self.logger.debug(path_dict_list)

      # construct ID based path
      # {{base_url}}/dataModels/{{data_model_id}}/dataClasses/{{data_class_id}}/dataElements/{{data_element_id}}
      id_based_url = ''

      j = r.json()
      breadcrumbs = j['breadcrumbs']
      self.logger.debug("Breadcrumb list: ")
      self.logger.debug(breadcrumbs)

      # add the returned entity onto the breadcrumbs for path traversing
      breadcrumbs.append(
        {
          'id': j['id'],
          'domainType': j['domainType'],
          'label': j['label']
        }
      )
      self.logger.debug("Breadcrumb list with searched entity: ")
      self.logger.debug(breadcrumbs)

      if (len(path_dict_list) != len(breadcrumbs)) :
        self.logger.debug("Path list length (" + str(len(path_dict_list)) + ") did not match breadcrumb list length (" + str(len(breadcrumbs)) + ")")
        return return_dict

      # Mauro 'get by ID' path is <modelID>/<nearest class to the entity>/<the entity>
      # meaning you need to get the model, then the last two things in the path chain.
      # but it still needs to go through the path to check the path is ok.
      id_based_url_element_list = []
      for idx, val in enumerate(path_dict_list):
        self.logger.debug("Comparing item: " + str(idx))
        self.logger.debug(str(val['entity_type']) + " ?= " + str(breadcrumbs[idx]['domainType']))
        self.logger.debug(str(val['entity_name']) + " ?= " + str(breadcrumbs[idx]['label']))

        if (
          val['entity_type'] == breadcrumbs[idx]['domainType']
          and val['entity_name'] == breadcrumbs[idx]['label']
        ):
          self.logger.debug("OK")
          

          # TODO this will need refactoring when other entities are required
          if breadcrumbs[idx]['domainType'] == 'DataModel' :
            # is the model finalised
            return_dict['model_finalised'] = breadcrumbs[idx]['finalised']
            id_based_url_element_list.append('/dataModels/' + breadcrumbs[idx]['id'])
          elif breadcrumbs[idx]['domainType'] == 'DataClass' :
            id_based_url_element_list.append('/dataClasses/' + breadcrumbs[idx]['id'])
          elif breadcrumbs[idx]['domainType'] == 'DataElement' :
            id_based_url_element_list.append('/dataElements/' + breadcrumbs[idx]['id'])
        else :
          self.logger.debug("match failed")
          return return_dict

      # if the function hasn't returned, the path is ok
      self.logger.debug("id_based_url_element_list")
      self.logger.debug(id_based_url_element_list)

      final_index = len(id_based_url_element_list)-1
      id_based_url = id_based_url_element_list[0] + id_based_url_element_list[final_index-1] + id_based_url_element_list[final_index]

      return_dict['url_found'] = True
      return_dict['id_based_url'] = id_based_url
      return return_dict

  # Takes a constructed path to an entity, suitable for the 'Edit Catalogue Item' endpoint of Mauro.
  #   (https://maurodatamapper.github.io/rest-api/resources/catalogue-item/)
  # Takes a string that will be updated onto the entity description.
  # Returns a dictionary :
  #   'status_code' : The numeric status of the http response
  #   'reason' : the response 'reason'
  #   'full_text' : the full response text
  def update_entity_description_by_id_path(self, path_to_entity, new_description):
    api_endpoint = '/'
    api_endpoint_path = api_endpoint + path_to_entity.lstrip("/")
    http_method = 'PUT'
    self.logger.debug("Attempting call to endpoint: " + str(api_endpoint_path))
    self.logger.debug("With http method: " + str(http_method))
    
    description_dict = {
      "description" : str(new_description)
    }
    call_body = json.dumps(description_dict)
    self.logger.debug("Call body:")
    self.logger.debug(call_body)
    
    r = self.call(api_endpoint_path, http_method, call_body)

    return_dict = {
      'status_code' : r.status_code,
      'reason' : r.reason,
      'full_text' : r.text
    }

    self.logger.debug("Response status code: " + str(r.status_code))
    self.logger.debug("Response text: " + r.text)

    return return_dict

