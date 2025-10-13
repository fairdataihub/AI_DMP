from pathlib import Path
import shutil

# Define your base data folder
base_dir = Path("C:/Users/Nahid/DMP-RAG/data")

# Define where you want all PDFs collected
ultimate_folder = base_dir / "all_pdfs"
ultimate_folder.mkdir(exist_ok=True)

# Loop through subfolders and find all PDFs recursively
for pdf_path in base_dir.rglob("*.pdf"):
    # Skip already moved PDFs
    if pdf_path.parent == ultimate_folder:
        continue

    # Keep original file name
    dest_path = ultimate_folder / pdf_path.name

    # Avoid overwriting files with the same name
    if dest_path.exists():
        counter = 1
        while True:
            new_name = dest_path.stem + f"_{counter}" + dest_path.suffix
            new_dest = ultimate_folder / new_name
            if not new_dest.exists():
                dest_path = new_dest
                break
            counter += 1

    # Copy the file
    shutil.copy2(pdf_path, dest_path)
    print(f"Copied: {pdf_path} → {dest_path}")

print(f"\n✅ All PDFs collected into: {ultimate_folder}")
