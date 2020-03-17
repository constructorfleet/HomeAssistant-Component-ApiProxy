"""
Connect two Home Assistant instances via the Websocket API.

For more details about this component, please refer to the documentation at
https://github.com/constructorfleet/HomeAssistant-Component-RemoteInstance
"""

import asyncio
import json
import logging
from json import JSONDecodeError

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from aiohttp import web, hdrs
from aiohttp.web import Response
from homeassistant.components import mqtt
from homeassistant.components.http import HomeAssistantView
from homeassistant.components.mqtt import valid_subscribe_topic, valid_publish_topic
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.typing import HomeAssistantType, ConfigType

_LOGGER = logging.getLogger(__name__)

ARG_SUBSCRIBE_ROUTE_TOPIC = 'subscribe_route_topic'
ARG_PUBLISH_REQUEST_ROUTES_TOPIC = 'publish_request_routes_topic'
ARG_INSTANCE_HOSTNAME_PREFIX = 'instance_hostname_prefix'
ARG_INSTANCE_HOSTNAME_CASING = 'instance_hostname_casing'

CASING_UPPER = 'UPPER'
CASING_LOWER = 'LOWER'
CASING_UNCHANGED = 'UNCHANGED'

DEFAULT_HOSTNAME_PREFIX = ''
DEFAULT_HOSTNAME_CASING = CASING_UNCHANGED

VALID_CASINGS = [
    CASING_UPPER,
    CASING_LOWER,
    CASING_UNCHANGED
]

DOMAIN = 'api_proxy'

EVENT_TYPE_REQUEST_ROUTES = 'request_routes'
EVENT_TYPE_ROUTE_REGISTERED = 'route_registered'

ATTR_ROUTE = 'route'
ATTR_METHOD = 'method'
ATTR_INSTANCE_NAME = 'instance_name'
ATTR_INSTANCE_IP = 'instance_ip'
ATTR_INSTANCE_PORT = 'instance_port'
ATTR_INSTANCE_HOSTNAME = 'instance_hostname'
ATTR_TOKEN = 'token'
ATTR_PROXY = 'proxy'
ATTR_RESPONSE = 'result'
ATTR_STATUS = 'status'
ATTR_BODY = 'body'
ATTR_EVENT_TYPE = "event_type"
ATTR_EVENT_DATA = "event_data"

DATA_PROXIES = 'proxies'

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

ROUTE_PREFIX_SERVICE_CALL = '/api/services/'

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Required(ARG_SUBSCRIBE_ROUTE_TOPIC): vol.All(
            cv.ensure_list,
            [valid_subscribe_topic]),
        vol.Optional(ARG_PUBLISH_REQUEST_ROUTES_TOPIC): valid_publish_topic,
        vol.Optional(ARG_INSTANCE_HOSTNAME_PREFIX,
                     default=DEFAULT_HOSTNAME_PREFIX): vol.Coerce(str),
        vol.Optional(ARG_INSTANCE_HOSTNAME_CASING, default=DEFAULT_HOSTNAME_CASING): vol.In(
            VALID_CASINGS)
    }),
}, extra=vol.ALLOW_EXTRA)


def _build_instance_hostname(instance_name, prefix, casing):
    concatenated_hostname = '%s%s' % (prefix, instance_name)
    if casing == CASING_LOWER:
        return concatenated_hostname.lower()
    elif casing == CASING_UPPER:
        return concatenated_hostname.upper()
    else:
        return concatenated_hostname


def _construct_api_proxy_class(hass, proxy_data):
    proxy_class = {
        'get': GetRemoteApiProxy,
        "post": PostRemoteApiProxy,
        "delete": DeleteRemoteApiProxy,
        "put": PutRemoteApiProxy,
        "patch": PatchRemoteApiProxy,
        "head": HeadRemoteApiProxy,
        "options": OptionsRemoteApiProxy
    }.get(proxy_data.method, None)

    if not proxy_class:
        return

    return proxy_class(
        hass,
        proxy_data
    )


async def async_setup(hass: HomeAssistantType, config: ConfigType):
    """Set up the api proxy component."""
    conf = config.get(DOMAIN)

    hass.data[DOMAIN] = {
        DATA_PROXIES: {}
    }

    for method in HTTP_METHODS:
        hass.data[DOMAIN][method] = {}

    def _register_proxy(proxy_api_event):
        """Registers a proxy received over MQTT."""
        proxy_route = proxy_api_event.get(ATTR_ROUTE)
        proxy_method = proxy_api_event.get(ATTR_METHOD, '').lower()
        proxy_instance_name = proxy_api_event.get(ATTR_INSTANCE_NAME)
        proxy_instance_port = proxy_api_event.get(ATTR_INSTANCE_PORT, 8123)
        if not proxy_route or \
                not proxy_method or \
                not proxy_instance_name or \
                proxy_route.startswith('http') or \
                '/local' in proxy_route:
            return
        proxy_data = ProxyData(
            hass,
            proxy_method,
            _build_instance_hostname(
                proxy_instance_name,
                conf.get(ARG_INSTANCE_HOSTNAME_PREFIX, ''),
                conf.get(ARG_INSTANCE_HOSTNAME_CASING, CASING_UNCHANGED)),
            proxy_instance_port,
            proxy_api_event.get(ATTR_TOKEN, None),
            proxy_route
        )
        existing_proxy = hass.data[DOMAIN].get(proxy_method, {}).get(proxy_route, None)
        if existing_proxy:
            existing_proxy.add_proxy(proxy_data)
        else:
            proxy_class = _construct_api_proxy_class(hass, proxy_data)

            hass.data[DOMAIN][proxy_method][proxy_route] = proxy_class
            if not proxy_route.startswith(ROUTE_PREFIX_SERVICE_CALL):
                for resource in [resource for resource in hass.http.app.router._resources if
                                 resource.canonical == proxy_route]:
                    hass.http.app.router._resources.remove(resource)
            hass.http.register_view(proxy_class)

    @callback
    def _event_receiver(msg):
        """Receive events published by and fire them on this hass instance."""
        event = json.loads(msg.payload)
        event_type = event.get(ATTR_EVENT_TYPE)
        event_data = event.get(ATTR_EVENT_DATA)

        if not event_type or event_type != EVENT_TYPE_ROUTE_REGISTERED:
            return

        _register_proxy(event_data)

    # Only subscribe if you specified a topic
    for topic in conf.get(ARG_SUBSCRIBE_ROUTE_TOPIC, []):
        await mqtt.async_subscribe(hass, topic, _event_receiver)

    if conf.get(ARG_PUBLISH_REQUEST_ROUTES_TOPIC):
        # Request remote instance routes on start up
        mqtt.async_publish(
            hass,
            conf[ARG_PUBLISH_REQUEST_ROUTES_TOPIC],
            json.dumps({
                ATTR_EVENT_TYPE: EVENT_TYPE_REQUEST_ROUTES,
                ATTR_EVENT_DATA: {}
            }),
            0,
            retain=True)

    return True


# pylint: disable=too-many-arguments
class ProxyData:
    """Container for proxy data."""

    def __init__(self, hass, method, host, port, token, route):
        self._hass = hass
        self.method = method
        self.host = host
        self.port = port
        self.token = token
        self.route = route
        self._session = async_get_clientsession(self._hass, False)

    def get_url(self, path):
        """Get route to connect to."""
        return '%s://%s:%s%s' % ('http', self.host, self.port, path)

    async def perform_proxy(self, request):
        """Forward request to the remote instance."""
        headers = {}
        if self.token is not None:
            headers[hdrs.AUTHORIZATION] = 'Bearer %s' % self.token

        proxy_url = self.get_url(request.path)

        request_method = getattr(self._session, self.method, None)
        if not request_method:
            _LOGGER.warning("Couldn't find method %s",
                            self.method)
            return Response(body="Proxy route not found", status=404)

        try:
            if self.method in HTTP_METHODS_WITH_PAYLOAD:
                result = await request_method(
                    proxy_url,
                    json=await request.json(),
                    params=request.query,
                    headers=headers
                )
            else:
                result = await request_method(
                    proxy_url,
                    params=request.query,
                    headers=headers
                )

            if result is not None:
                return await self._convert_response(result)
        except Exception as e:
            _LOGGER.error(
                "Error proxying %s %s to %s: %s",
                self.method,
                request.url,
                proxy_url,
                str(e)
            )
        return Response(body="Unable to proxy request", status=500)

    async def _convert_response(self, client_response):
        if 'json' in client_response.headers.get(hdrs.CONTENT_TYPE, '').lower():
            response_body = await client_response.read()
            try:
                data = json.loads(response_body)
                return {
                    ATTR_PROXY: self,
                    ATTR_RESPONSE: data,
                    ATTR_STATUS: client_response.status
                }
            except JSONDecodeError:
                return {
                    ATTR_PROXY: self,
                    ATTR_RESPONSE: "Unable to parse JSON",
                    ATTR_STATUS: 500
                }
        return {
            ATTR_PROXY: self,
            ATTR_RESPONSE: Response(
                body=client_response.content,
                status=client_response.status,
                headers=client_response.headers),
            ATTR_STATUS: client_response.status
        }

    def copy_with_route(self, route):
        """Creates a new ProxyData with the specified route."""
        return ProxyData(
            self._hass,
            self.method,
            self.host,
            self.port,
            self.token,
            route
        )

    def is_exact_match(self, method, route):
        """Checks if the method and route are an exact match."""
        return self.method == method and self.route == route

    def __eq__(self, other):
        if isinstance(other, ProxyData):
            return self.host == other.host \
                   and self.port == other.port \
                   and self.method == other.method
        return False

    def __hash__(self):
        return hash(self.__repr__())

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return "%s %s%s%s" % (self.method, self.host, self.port, self.route)


class AbstractRemoteApiProxy(HomeAssistantView):
    """A proxy for remote API calls."""

    cors_allowed = True

    def __init__(self, hass, proxy):
        """Initializing the proxy."""
        if proxy.method not in HTTP_METHODS:
            return

        self.proxies = set()
        self.requires_auth = False
        self.url = proxy.route if str(proxy.route).startswith('/') else '/%s' % proxy.route
        self.name = self.url.replace('/', ':')[1:]
        self._hass = hass
        self._method = proxy.method

        self._session = proxy._session
        self.add_proxy(proxy.copy_with_route(self.url))

    def add_proxy(self,
                  proxy):
        """Adds a proxy to the set."""
        self.proxies.remove(proxy)
        self.proxies.add(proxy)

    async def perform_proxy(self, request, **kwargs):
        """Proxies the request to the remote instance."""
        route = request.url.path
        exact_match_proxies = [proxy for proxy in self.proxies if
                               proxy.is_exact_match(self._method, route)]
        if len(exact_match_proxies) != 0:
            _LOGGER.warning("Found %s proxies for %s",
                            str(exact_match_proxies),
                            route)
            results = await asyncio.gather(
                *[proxy.perform_proxy(request) for proxy in exact_match_proxies])
        else:
            _LOGGER.warning("Using %s proxies for %s",
                            str(self.proxies),
                            route)
            results = await asyncio.gather(
                *[proxy.perform_proxy(request) for proxy in self.proxies])

        for result in results:
            if result[ATTR_STATUS] == 200:
                if not route.startswith(ROUTE_PREFIX_SERVICE_CALL):
                    proxy = result[ATTR_PROXY]
                    exact_proxy = proxy.copy_with_route(route)
                    self.proxies.add(exact_proxy)
                if isinstance(result[ATTR_RESPONSE], web.StreamResponse):
                    return result[ATTR_RESPONSE]
                return self.json(result[ATTR_RESPONSE])

        return self.json_message("Unable to proxy request", 500)


class GetRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy GET requests."""

    async def get(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)


class PostRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy POST requests."""

    async def post(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)


class PutRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy PUT requests."""

    async def put(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)


class DeleteRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy DELETE requests."""

    async def delete(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)


class PatchRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy PATCH requests."""

    async def delete(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)


class HeadRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy HEAD requests."""

    async def head(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)


class OptionsRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy OPTIONS requests."""

    async def options(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)
