def compare(t1, t2):
    return t1[0]-t2[0]

ls = [(1,2),(3,4),(-1,10)]

ls_sorted = sorted(ls, key=lambda x: x[0])

print ls
print ls_sorted

a,b = zip(*ls_sorted)

print a
print b