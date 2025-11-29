#!/usr/bin/env python3
print("Content-Type: text/html\n")  # HTTP header

from ftplib import FTP

# FTP credentials
ftp_host = "ftp.sahilkumar.in"
ftp_user = "sushilpwc@sahilkumar.in"
ftp_pass = "SahilPahuja@29"  # Make sure password is correct

# Connect to FTP server
ftp = FTP(ftp_host)
ftp.login(user=ftp_user, passwd=ftp_pass)

# Change to the desired directory
ftp.cwd('/csv')  # Folder path on FTP

# Get list of files
files = ftp.nlst()

# Output HTML
# print("<html><body>")
# print("<h2>Files in /csv folder:</h2>")
# print("<ul>")
# for file in files:
#     print(f"<li>{file}</li>")
# print("</ul>")
# print("</body></html>")

# Close the connection
ftp.quit()

return files
