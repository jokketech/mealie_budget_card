from __future__ import annotations
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.const import CONF_NAME
from homeassistant.helpers import selector
from .const import *

class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input=None):
        errors = {}
        if user_input is not None:
            await self.async_set_unique_id("mealie_budget_default")
            self._abort_if_unique_id_configured()
            return self.async_create_entry(title=user_input[CONF_NAME], data=user_input)
        mealie_entries = {
            entry.entry_id: entry.title
            for entry in self.hass.config_entries.async_entries("mealie")
        }
        schema = vol.Schema({
            vol.Required(CONF_NAME, default="Mealie Budget"): str,
            vol.Required(CONF_MEALIE_CONFIG_ENTRY_ID): selector.SelectSelector(
                selector.SelectSelectorConfig(options=[
                    selector.SelectOptionDict(value=k, label=v) for k,v in mealie_entries.items()
                ], mode=selector.SelectSelectorMode.DROPDOWN)
            ),
            vol.Required(CONF_MEALIE_URL, default="http://87.54.92.227:30111"): str,
            vol.Required(CONF_SALLING_TOKEN): selector.TextSelector(
                selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
            ),
            vol.Required(CONF_CATEGORY, default=DEFAULT_CATEGORY): str,
            vol.Required(CONF_SERVINGS, default=DEFAULT_SERVINGS): vol.All(int, vol.Range(min=1,max=12)),
            vol.Required(CONF_MAX_RECIPES, default=DEFAULT_MAX_RECIPES): vol.All(int, vol.Range(min=1,max=50)),
        })
        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)
