def to_int(s: str) -> int:
    return int(s.replace(",", ""))


def is_reply(comments: list) -> list:
    res = [False]
    if len(comments) > 0:
        res.extend([True for _ in comments])
    return res
