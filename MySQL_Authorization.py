def MySQL_Auth():
    token = None
    with open("password.txt") as I:
        token = I.read().strip()
    return token