"""Match the destination a crew typed into AIS to one of our ports.

Free text, 20 characters: 'LONG BEACH', 'USLGB', 'US LGB', 'LGB ANCH', 'CNSHA>USLAX', 'FOR ORDERS'.
A trip written as 'FROM>TO' is read after the last '>'. Port name, UN/LOCODE (with or without the space)
and a few common short forms match; anything else stays unmatched and the dashboard infers the destination.
"""

import re

from common.geo import Port

# ponytail: hand-kept short forms for the commonest spellings; a full UN/LOCODE alias table if more are needed
EXTRA = {
    "USLAX": ("LA", "LAX", "L A", "SAN PEDRO", "LOS ANGELES CA"),
    "USLGB": ("LB", "LGB", "LONG BEACH CA"),
    "SGSIN": ("SG", "SIN", "SPORE", "S PORE", "SINGAPORE PSA", "SINGAPORE EOPL"),
    "HKHKG": ("HK", "HKG", "HONGKONG"),
    "CNSHA": ("SHANGHAI CHINA", "SHA"),
    "KRPUS": ("PUSAN", "BUSAN KOREA"),
    "JPYOK": ("YOKOHAMA JAPAN", "YOK"),
    "JPTYO": ("TOKYO JAPAN",),
    "USSEA": ("SEATTLE WA", "SEATTLE USA"),
    "USOAK": ("OAKLAND CA",),
    "USSFO": ("SAN FRANCISCO CA", "SAN FRAN"),
    "CAVAN": ("VANCOUVER BC",),
    "USHNL": ("HONOLULU HI",),
}
ORDERS = ("FOR ORDERS", "FOR ORDER", "ORDERS", "TBA", "TBN", "AWAITING ORDERS")


def clean(text: str | None) -> str:
    """The part after the last '>' (or '->', '=>'), letters and digits only, single spaces."""
    last = re.split(r"=?-?>", (text or "").upper())[-1]
    return " ".join(re.sub(r"[^A-Z0-9 ]", " ", last).split())


def is_orders(text: str | None) -> bool:
    """'FOR ORDERS' and friends: the charterer has not said where the ship goes yet."""
    t = clean(text)
    return any(t == o or t.startswith(o + " ") for o in ORDERS)


def match(text: str | None, ports: dict[str, Port]) -> str | None:
    """The port_id the declared destination names, or None."""
    t = clean(text)
    if not t:
        return None
    for port in ports.values():
        code = port.port_id
        names = (code, f"{code[:2]} {code[2:]}", clean(port.name), *EXTRA.get(code, ()))
        # 'LGB ANCH', 'LONG BEACH CA USA': a name may be followed by more words, never preceded. Two-letter
        # forms only match alone, so 'LA PAZ' is not Los Angeles.
        if any(t == n or (len(n) > 2 and t.startswith(n + " ")) for n in names):
            return code
    return None
