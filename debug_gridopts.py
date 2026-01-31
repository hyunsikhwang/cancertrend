from pyecharts import options as opts
import inspect

try:
    print("GridOpts init args:", inspect.getfullargspec(opts.GridOpts.__init__).args)
except Exception as e:
    print("Error:", e)

g = opts.GridOpts()
print("GridOpts object dict keys:", g.opts.keys() if hasattr(g, 'opts') else "No opts attr")
