def messenger(year):
    prefix = "(%s)" % year
    def foo(*args):
        print (prefix, *args)
    return foo
