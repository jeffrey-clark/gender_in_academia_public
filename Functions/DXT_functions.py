import re

def blanks_separate(doc):
    blank_count = 0
    for i in range(0, len(doc)):
        row = doc[i]
        if row.blank == True:
            blank_count = blank_count + 1
    blank_percentage = blank_count / len(doc)
    if blank_percentage > 0.2:
        return True
    else:
        return False


# def blanks_separate_section():



def check_citation_bragger(row):

    # strong triggers will skip even if author name is present
    strong_triggers = [
        r"asterisk.*denotes"
    ]

    weak_triggers = [
        r"\d+\s+citations*",  # Cited until March 2016 (CT). In total: 503 citations.
        r"according.+google\s*scholar",
    ]

    for tp in strong_triggers:
        trigger_match = re.search(tp, row.text.lower())
        if trigger_match != None:
            return "strong"

    # if no match on strong triggers check the weak triggers
    for tp in weak_triggers:
        trigger_match = re.search(tp, row.text.lower())
        if trigger_match != None:
            return "weak"

    return None

