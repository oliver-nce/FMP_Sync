__version__ = "2.0.9"

import frappe
import frappe.email.queue as _email_queue

_original_get_unsubscribe_message = _email_queue.get_unsubscribe_message


def _patched_get_unsubscribe_message(unsubscribe_message, expose_recipients):
	"""Suppress unsubscribe footer when Email Account has it disabled.

	Frappe's original treats empty string as falsy and falls back to
	the default "Unsubscribe" text.  This patch honours the empty string
	passed by _notify() when send_unsubscribe_message is unchecked.
	"""
	if unsubscribe_message and unsubscribe_message.strip():
		return _original_get_unsubscribe_message(unsubscribe_message, expose_recipients)
	return frappe._dict(html="", text="")


_email_queue.get_unsubscribe_message = _patched_get_unsubscribe_message
