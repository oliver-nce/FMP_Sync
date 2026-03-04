__version__ = "2.1.0"

import frappe
import frappe.email.queue as _email_queue
from frappe.core.doctype.communication import mixins as _comm_mixins

# ---------------------------------------------------------------------------
# Patch 1: Suppress unsubscribe footer when Email Account has it disabled.
# Frappe treats empty string as falsy and falls back to the default
# "Unsubscribe" text.  This honours the empty string passed by _notify().
# ---------------------------------------------------------------------------
_original_get_unsubscribe_message = _email_queue.get_unsubscribe_message


def _patched_get_unsubscribe_message(unsubscribe_message, expose_recipients):
	if unsubscribe_message and unsubscribe_message.strip():
		return _original_get_unsubscribe_message(unsubscribe_message, expose_recipients)
	return frappe._dict(html="", text="")


_email_queue.get_unsubscribe_message = _patched_get_unsubscribe_message

# ---------------------------------------------------------------------------
# Patch 2: Strip Reply-To header entirely.  Without an incoming email
# account Frappe falls back to the session user's email which is wrong.
# Removing Reply-To lets mail clients reply to the From address instead.
# ---------------------------------------------------------------------------
_original_sendmail_input_dict = _comm_mixins.CommunicationEmailMixin.sendmail_input_dict


def _patched_sendmail_input_dict(self, **kwargs):
	result = _original_sendmail_input_dict(self, **kwargs)
	if result:
		result["reply_to"] = None
	return result


_comm_mixins.CommunicationEmailMixin.sendmail_input_dict = _patched_sendmail_input_dict
