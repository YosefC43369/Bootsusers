# การ Build เป็นไฟล์ .exe (Windows)

> **สำคัญ:** PyInstaller ไม่ใช่ cross-compiler — มันสร้างไฟล์ปฏิบัติการสำหรับ
> ระบบปฏิบัติการที่รันมันเท่านั้น ดังนั้น **ต้อง build บน Windows** จึงจะได้
> `.exe` ที่รันบน Windows ได้ (build บน Linux/mac จะได้ไบนารีของระบบนั้น ๆ)

## วิธีที่ 1 — กดครั้งเดียว (แนะนำ)

1. ติดตั้ง **Python 3.9+** จาก [python.org](https://www.python.org/downloads/)
   (ตอนติดตั้งให้ติ๊ก *Add python.exe to PATH* — ตัวติดตั้งนี้มี `tkinter`
   มาให้ ซึ่ง `customtkinter` ต้องใช้)
2. ดับเบิลคลิก **`build_exe.bat`**
3. ได้ไฟล์ที่ **`dist\Bootsusers.exe`** (ไฟล์เดียว ไม่มีหน้าต่าง console)

สคริปต์จะสร้าง venv แยก ติดตั้ง dependencies + PyInstaller และ build ให้อัตโนมัติ

## วิธีที่ 2 — สั่งเอง

```bat
pip install -r requirements.txt
pip install pyinstaller
pyinstaller --noconfirm --clean bootsusers.spec
```

ผลลัพธ์: `dist\Bootsusers.exe`

## สิ่งที่ถูกฝังในไฟล์ .exe

- `assets\Hack-Regular.ttf` — ฟอนต์ Hack (โหลดอัตโนมัติตอนรัน ไม่ต้องติดตั้งฟอนต์)
- data files ของ `customtkinter` (ธีม/asset)
- โมดูลย่อยของ `telethon` + `PySocks`

## ไอคอนเอง (ไม่บังคับ)

วางไฟล์ `assets\icon.ico` ก่อน build แล้ว `bootsusers.spec` จะหยิบไปใช้ให้เอง
ถ้าไม่มี ก็ใช้ไอคอนเริ่มต้นของ PyInstaller

## การใช้งานหลัง build

- ดับเบิลคลิก `Bootsusers.exe` → เปิด GUI (ธีม cybersecurity เขียว ฟอนต์ Hack)
- วางไฟล์ `.csv` (คอลัมน์ id / email / phone ...) ไว้ในโฟลเดอร์ `accounts`
  ข้าง ๆ ตัว `.exe` แล้วกด **LOAD → CONNECT** จากนั้นเลือกงาน (JOIN / REACT /
  STARS / VIEWS)
- โหมด headless: `Bootsusers.exe --cli views --channel @c --message latest --count 50`
