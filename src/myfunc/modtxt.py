def capture(x: str, *targets: str, markers=('<<<', '>>>')):
    for target in targets:
        wrapped_target = markers[0] + target + markers[1]
        x = x.replace(target, wrapped_target)
    return x
