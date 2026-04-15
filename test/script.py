from pathlib import Path

current_dir = Path.cwd() # Get the current working directory
current_file = Path(__file__).name # Get the name of the current file

print(f"Files in {current_dir}:")

for filepath in current_dir.iterdir(): 
    if filepath.name == current_file:
        continue
    print(filepath.name)

    if filepath.is_file():
        content = filepath.read_text()
        print(f"Content of {filepath.name}:\n{content}\n")

