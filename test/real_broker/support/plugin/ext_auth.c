/*
 * A mosquitto 2.x plugin implementing two MQTT 5 enhanced-authentication
 * methods, so the client's AUTH handling can be exercised against a real
 * broker:
 *
 *   TEST-CR   CONNECT data "client-first"     -> AUTH 0x18 "server-challenge"
 *             AUTH data "proof:server-challenge" -> CONNACK 0x00 data "server-final"
 *             anything else                    -> rejected (0x87)
 *             Re-authentication (AUTH 0x19) runs the same exchange; data
 *             "client-first-reauth" is accepted, "deny" is rejected.
 *   TEST-ONE  CONNECT data "ok" -> CONNACK 0x00 data "welcome" (no AUTH round)
 */
#include <stdlib.h>
#include <string.h>
#include <mosquitto.h>
#include <mosquitto/broker.h>
#include <mosquitto/broker_plugin.h>
#include <mosquitto/libcommon_memory.h>

MOSQUITTO_PLUGIN_DECLARE_VERSION(5);

static mosquitto_plugin_id_t *plugin_id;

static int data_is(const struct mosquitto_evt_extended_auth *ed, const char *s)
{
	size_t n = strlen(s);
	return ed->data_in_len == n && (n == 0 || memcmp(ed->data_in, s, n) == 0);
}

static void reply(struct mosquitto_evt_extended_auth *ed, const char *s)
{
	size_t n = strlen(s);
	ed->data_out = mosquitto_malloc(n);
	memcpy(ed->data_out, s, n);
	ed->data_out_len = (uint16_t)n;
}

static int on_start(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_extended_auth *ed = event_data;
	(void)event; (void)userdata;
	if(ed->auth_method == NULL) return MOSQ_ERR_PLUGIN_DEFER;
	if(!strcmp(ed->auth_method, "TEST-ONE")){
		if(data_is(ed, "ok")){
			reply(ed, "welcome");
			return MOSQ_ERR_SUCCESS;
		}
		return MOSQ_ERR_AUTH;
	}
	if(!strcmp(ed->auth_method, "TEST-CR")){
		if(data_is(ed, "client-first") || data_is(ed, "client-first-reauth")){
			reply(ed, "server-challenge");
			return MOSQ_ERR_AUTH_CONTINUE;
		}
		return MOSQ_ERR_AUTH;
	}
	return MOSQ_ERR_PLUGIN_DEFER;
}

static int on_continue(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_extended_auth *ed = event_data;
	(void)event; (void)userdata;
	if(ed->auth_method == NULL || strcmp(ed->auth_method, "TEST-CR")){
		return MOSQ_ERR_PLUGIN_DEFER;
	}
	if(data_is(ed, "proof:server-challenge")){
		reply(ed, "server-final");
		return MOSQ_ERR_SUCCESS;
	}
	return MOSQ_ERR_AUTH;
}

int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **userdata,
		struct mosquitto_opt *opts, int opt_count)
{
	(void)userdata; (void)opts; (void)opt_count;
	plugin_id = identifier;
	mosquitto_callback_register(plugin_id, MOSQ_EVT_EXT_AUTH_START, on_start, NULL, NULL);
	mosquitto_callback_register(plugin_id, MOSQ_EVT_EXT_AUTH_CONTINUE, on_continue, NULL, NULL);
	return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_cleanup(void *userdata, struct mosquitto_opt *opts, int opt_count)
{
	(void)userdata; (void)opts; (void)opt_count;
	return MOSQ_ERR_SUCCESS;
}
