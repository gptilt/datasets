def roman_to_integer(roman: str) -> int:
    roman_numerals = {
        'I': 1,
        'V': 5,
        'X': 10,
        'L': 50,
        'C': 100,
        'D': 500,
        'M': 1000
    }
    integer_value = 0
    prev_value = 0

    for char in reversed(roman):
        curr_value = roman_numerals.get(char, 0)
        if curr_value < prev_value:
            integer_value -= curr_value
        else:
            integer_value += curr_value
        prev_value = curr_value
    return integer_value
