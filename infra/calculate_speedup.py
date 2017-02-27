no_idx_str = "149512 & 156134 & 2358 & 5581 & 792 & 879 & 725 & 866"
no_idx_ls = no_idx_str.split(" & ")
no_idx_enum = [e for e in enumerate(no_idx_ls)]

baseline = [float(t[1]) for t in no_idx_enum if not t[0] % 2]
a_idx_str = "160232 & 306038 & 7 & 7 & 5668 & 6886 & 1 & 3"
b_idx_str = "194849 & 308241 & 3456 & 5009 & 1 & 1 & 1 & 1"
ab_idx_str = "314587 & 564138 & 10 & 97 & 9 & 51 & 0 & 1"
row_lists = [ls.split(' & ') for ls in [no_idx_str, a_idx_str, b_idx_str, ab_idx_str]]

enumer = [[e for e in enumerate(x)] for x in row_lists]
base_index_list = [[(t[0]//2, float(t[1])) for t in x] for x in enumer]
speedup=[[str(y[1]/baseline[y[0]])[:6] for y in x] for x in base_index_list]
speedup_rvr=[[str(baseline[y[0]]/y[1]) if y[1] != 0 else "NaN" for y in x] for x in base_index_list]
form = lambda x: [print(" & ".join(y), "\n") for y in x]
form(speedup)
form(speedup_rvr)

# speedup_format = [" & ".join(x) for x in speedup]
#print(form(speedup))
#print(form(speedup_rvr))
