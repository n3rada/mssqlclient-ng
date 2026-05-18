import ast
from pathlib import Path

actions_dir = Path("/home/kg6412@eit-lab.local/git/mssqlclient-ng/src/mssqlclient_ng/core/actions")

for path in sorted(actions_dir.rglob("*.py")):
    try:
        source = path.read_text()
        tree = ast.parse(source)
    except Exception:
        continue
    
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "execute":
            # Check if it has a convert_list_of_dicts call
            has_convert = any(
                isinstance(n, ast.Call) and 
                isinstance(n.func, ast.Attribute) and 
                n.func.attr == "convert_list_of_dicts"
                for n in ast.walk(node)
            )
            if not has_convert:
                continue
            
            # Check all return statements
            returns = [n for n in ast.walk(node) if isinstance(n, ast.Return)]
            all_none = all(
                r.value is None or (isinstance(r.value, ast.Constant) and r.value.value is None)
                for r in returns
            )
            # Also check if there are NO return statements (implicit None)
            if all_none or not returns:
                print(f"{path.relative_to('/home/kg6412@eit-lab.local/git/mssqlclient-ng')} — returns None, has convert_list_of_dicts")
