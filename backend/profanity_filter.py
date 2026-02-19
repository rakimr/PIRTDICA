import re

BANNED_WORDS = [
    "nigger", "nigga", "nigg", "n1gger", "n1gga", "n1gg",
    "faggot", "fagg", "f4ggot", "f4g",
    "retard", "retarded", "r3tard",
    "chink", "ch1nk",
    "spic", "sp1c",
    "wetback", "w3tback",
    "kike", "k1ke",
    "coon", "c00n",
    "gook", "g00k",
    "beaner", "b3aner",
    "towelhead", "raghead",
    "cracker", "cracka",
    "tranny", "tr4nny",
    "dyke", "dyk3",
    "nazi", "n4zi", "naz1",
    "hitler", "h1tler",
    "kkk",
    "jihad", "j1had",
    "rape", "r4pe", "rapist", "rap1st",
    "molest", "mol3st", "pedophile", "pedo", "p3do",
    "fuck", "fuk", "fvck", "f0ck", "fck", "fuq",
    "shit", "sh1t", "sht",
    "bitch", "b1tch", "biatch",
    "cunt", "cvnt", "c0nt",
    "cock", "c0ck", "d1ck", "dick",
    "pussy", "pvssy", "pvss",
    "ass", "a55",
    "whore", "wh0re", "h0e", "hoe",
    "slut", "sl0t", "slutt",
    "bastard", "b4stard",
    "damn", "d4mn",
    "negro",
    "honky", "h0nky",
    "chinaman",
    "jap",
    "sandnigger",
    "shemale",
    "wop", "w0p",
    "dago", "dag0",
    "kyke",
    "lesbo", "l3sbo",
    "homo", "h0mo",
    "skank", "sk4nk",
    "thot", "th0t",
    "incel", "1ncel",
    "simp",
    "cuck", "cvck",
    "cumslut", "cumdump",
    "blowjob", "bl0wjob",
    "handjob",
    "pornhub", "xvideos", "xhamster",
    "onlyfans",
    "isis",
    "alqaeda", "al-qaeda",
    "suicide", "su1cide", "kms", "kys",
    "school shooter", "massmurder",
    "genocide", "gen0cide",
    "holocaust",
    "slavery",
    "lynching",
    "terrorist", "terr0rist",
    "bomb", "b0mb",
    "cocaine", "c0caine", "heroin", "her0in", "meth",
    "crack",
]

LEET_MAP = {
    "0": "o",
    "1": "i",
    "3": "e",
    "4": "a",
    "5": "s",
    "7": "t",
    "8": "b",
    "@": "a",
    "$": "s",
    "!": "i",
    "|": "l",
}


def _normalize_text(text):
    text = text.lower().strip()
    normalized = ""
    for ch in text:
        normalized += LEET_MAP.get(ch, ch)
    normalized = re.sub(r"[^a-z]", "", normalized)
    return normalized


def _dedup_chars(text):
    result = ""
    for ch in text:
        if not result or ch != result[-1]:
            result += ch
    return result


def check_username(username):
    if not username or not isinstance(username, str):
        return False, "Username is required"

    if len(username) < 3:
        return False, "Username must be at least 3 characters"

    if len(username) > 30:
        return False, "Username must be 30 characters or less"

    if not re.match(r'^[a-zA-Z0-9_\-]+$', username):
        return False, "Username can only contain letters, numbers, underscores, and hyphens"

    normalized = _normalize_text(username)
    deduped = _dedup_chars(normalized)

    for word in BANNED_WORDS:
        clean_word = _normalize_text(word)
        deduped_word = _dedup_chars(clean_word)
        if clean_word in normalized or deduped_word in deduped:
            return False, "That username contains inappropriate language. Please choose another."

    return True, None


def scan_usernames(usernames):
    flagged = []
    for username in usernames:
        is_valid, reason = check_username(username)
        if not is_valid and reason != "Username is required":
            flagged.append({"username": username, "reason": reason})
    return flagged
