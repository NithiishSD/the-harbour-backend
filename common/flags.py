"""A ship's flag country from its MMSI: the first three digits are the Maritime Identification Digits.

Used to tell "ships of this country" from foreign ships, and to judge where an inbound ship comes from when
its source port was not seen. Only the countries this dashboard knows ports of and the usual open
registries."""

MID_TO_COUNTRY: dict[int, str] = {}
for _country, _mids in {
    "SG": (563, 564, 565, 566), "MY": (533,), "ID": (525,), "TH": (567,), "VN": (574,), "PH": (548,),
    "CN": (412, 413, 414), "HK": (477,), "TW": (416,), "KR": (440, 441), "JP": (431, 432),
    "US": (338, 366, 367, 368, 369), "CA": (316,), "MX": (345,),
    "PA": (351, 352, 353, 354, 355, 356, 357, 370, 371, 372, 373, 374),
    "IN": (419,), "LK": (417,), "AE": (470, 471), "OM": (461,), "SA": (403,), "YE": (473, 475), "EG": (622,),
    "GR": (237, 239, 240, 241), "ES": (224, 225), "IT": (247,), "FR": (226, 227, 228),
    "MT": (215, 229, 248, 249, 256), "PT": (204, 255, 263), "NL": (244, 245, 246), "BE": (205,),
    "DE": (211, 218), "GB": (232, 233, 234, 235), "BR": (710,), "ZA": (601,), "AU": (503,), "NZ": (512,),
    "LR": (636, 637), "MH": (538,), "BS": (308, 309, 311),
}.items():  # fmt: skip
    for _mid in _mids:
        MID_TO_COUNTRY[_mid] = _country


def flag_country(mmsi: str) -> str | None:
    """The ISO country code of the flag, or None for an MMSI that is not a ship's or a country not listed."""
    if len(mmsi) != 9 or not mmsi.isdigit():
        return None
    return MID_TO_COUNTRY.get(int(mmsi[:3]))
