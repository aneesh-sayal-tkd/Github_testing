import os
import sys
import pandas


print(" World")
a = 10
b = 20
c= a+b

print(a)
x=40
def read_user_file(filename):
    # VULNERABLE: No path validation
    file_path = f"/var/www/uploads/{filename}"
    with open(file_path, 'r') as f:
        return f.read()

def save_uploaded_file(filename, content):
    # VULNERABLE: Directory traversal possible
    full_path = os.path.join("/uploads", filename)
    with open(full_path, 'w') as f:
        f.write(content)

def delete_file(filename):
    # VULNERABLE: Unrestricted file deletion
    os.remove(f"/tmp/{filename}")
