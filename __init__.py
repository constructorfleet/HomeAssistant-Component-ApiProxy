"""
Proxy requests through this instance to other instances in the cluster.

Requires https://github.com/constructorfleet/HomeAssistant-Component-ViewEvent to be running on the
remote instances.

For more details about this component, please refer to the documentation at
https://github.com/constructorfleet/HomeAssistant-Component-RemoteInstance
"""
import json
import logging

import voluptuous as vol
from homeassistant.components.apple_tv import ensure_list

from homeassistant.core import callback, EventOrigin
from homeassistant.components.mqtt import valid_subscribe_topic
from homeassistant.helpers.typing import HomeAssistantType, ConfigType

_LOGGER = logging.getLogger(__name__)

DOMAIN = 'api_proxy'

CONF_ROUTE_REGISTERED_SUBSCRIBE_TOPIC = 'subscribe_route_registered_topic'
CONF_IGNORE_LOCAL_EVENTS = 'ignore_local_events'
CONF_IGNORE_REMOTE_EVENTS = 'ignore_remote_events'

ATTR_EVENT_TYPE = 'event_type'
ATTR_EVENT_DATA = 'event_data'

DATA_PROXIES = 'proxies'

EVENT_TYPE_ROUTE_REGISTERED = 'route_registered'

HTTP_METHODS = [
    "get",
    "post",
    "delete",
    "put",
    "patch",
    "head",
    "options"
]
HTTP_METHODS_WITH_PAYLOAD = [
    'post',
    'put',
    'patch'
]

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Exclusive(CONF_IGNORE_LOCAL_EVENTS, 'ignore_event'):  vol.Boolean,
        vol.Exclusive(CONF_IGNORE_REMOTE_EVENTS, 'ignore_event'):  vol.Boolean
    })
})


async def async_setup(hass: HomeAssistantType, config: ConfigType):
    """Set up the remote_homeassistant component."""

    hass.data[DOMAIN] = {
        DATA_PROXIES: {}
    }

    for method in HTTP_METHODS:
        hass.data[DOMAIN][method] = {}

    conf = config[DOMAIN]
    ignore_local = conf[CONF_IGNORE_LOCAL_EVENTS]
    ignore_remote = conf[CONF_IGNORE_REMOTE_EVENTS]

    if ignore_local and ignore_remote:
        _LOGGER.error('Local and remote events are ignored, nothing to see here')
        return

    @callback
    def _route_registered_receiver(event):
        if ignore_local and event.origin == EventOrigin.local:
            return
        if ignore_remote and event.origin == EventOrigin.remote:
            return

        # TODO : Validate payload, register proxy, insert into http router

    hass.bus.async_listen(EVENT_TYPE_ROUTE_REGISTERED, _route_registered_receiver)

    return True
