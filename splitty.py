import os


def split_file(file_path, chunk_size_kb=64):
    chunk_size_bytes = chunk_size_kb * 1024  # Convert KB to bytes
    part_num = 1

    with open(file_path, 'r', encoding='utf-8') as file:
        chunk = file.read(chunk_size_bytes)
        while chunk:
            part_name = f"{file_path}_part_{part_num}"
            with open(part_name, 'w', encoding='utf-8') as part_file:
                part_file.write(chunk)
            part_num += 1
            chunk = file.read(chunk_size_bytes)


def split_all_files_in_folder(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith(".txt"):
            file_path = os.path.join(folder_path, filename)
            split_file(file_path)


# Example usage
folder_path = './books'
split_all_files_in_folder(folder_path)
