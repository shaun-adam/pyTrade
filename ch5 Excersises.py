def check_fermet(n):
    r = range(1,101)
    for a in r:
        for b in r:
            for c in r:
                left = a**n + b**n
                right = c**n
                if left == right:
                    print("found match: a: ",a," b: ",b," c: ",c," left: ",left," right: ",right)

check_fermet(2)
