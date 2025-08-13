# app/utils/persian_tools.py
import logging

logger = logging.getLogger(__name__)

_PERSIAN_ZERO = "صفر"
_PERSIAN_NEGATIVE = "منفی"
_PERSIAN_WORDS = {
    0: "", 1: "یک", 2: "دو", 3: "سه", 4: "چهار", 5: "پنج", 6: "شش", 7: "هفت", 8: "هشت", 9: "نه",
    10: "ده", 11: "یازده", 12: "دوازده", 13: "سیزده", 14: "چهارده", 15: "پانزده", 16: "شانزده", 17: "هفده", 18: "هجده", 19: "نوزده",
    20: "بیست", 30: "سی", 40: "چهل", 50: "پنجاه", 60: "شصت", 70: "هفتاد", 80: "هشتاد", 90: "نود"
}
_PERSIAN_HUNDREDS = {
    100: "صد", 200: "دویست", 300: "سیصد", 400: "چهارصد", 500: "پانصد",
    600: "ششصد", 700: "هفتصد", 800: "هشتصد", 900: "نهصد"
}
_PERSIAN_SEPARATORS = {
    1000: "هزار",
    1000000: "میلیون",
    1000000000: "میلیارد",
    1000000000000: "تریلیون",
}

def _three_digit_to_word(n):
    """Converts a number from 0-999 to its Persian word representation."""
    if n < 20:
        return _PERSIAN_WORDS.get(n, "")
    if n < 100:
        tens, ones = divmod(n, 10)
        tens_word = _PERSIAN_WORDS.get(tens * 10, "")
        ones_word = _PERSIAN_WORDS.get(ones, "")
        return f"{tens_word} و {ones_word}" if ones_word else tens_word
    if n < 1000:
        hundreds, rem = divmod(n, 100)
        hundreds_word = _PERSIAN_HUNDREDS.get(hundreds * 100, "")
        if rem == 0:
            return hundreds_word
        else:
            return f"{hundreds_word} و {_three_digit_to_word(rem)}"
    return ""

def convert_amount_to_persian_word(num):
    """
    Converts a given integer to its Persian word representation.
    For example: 12345 -> "دوازده هزار و سیصد و چهل و پنج"
    """
    if not isinstance(num, (int, float)):
        try:
            num = int(num)
        except (ValueError, TypeError):
            logger.warning(f"Cannot convert '{num}' to integer for word conversion.")
            return ""

    num = int(num) # ensure it's an integer

    if num == 0:
        return _PERSIAN_ZERO

    if num < 0:
        return f"{_PERSIAN_NEGATIVE} {convert_amount_to_persian_word(abs(num))}"

    parts = []
    sorted_separators = sorted(_PERSIAN_SEPARATORS.keys(), reverse=True)
    
    temp_num = num
    for sep_val in sorted_separators:
        if temp_num >= sep_val:
            count, temp_num = divmod(temp_num, sep_val)
            parts.append(f"{_three_digit_to_word(count)} {_PERSIAN_SEPARATORS[sep_val]}")

    if temp_num > 0:
        parts.append(_three_digit_to_word(temp_num))

    return " و ".join(filter(None, parts))