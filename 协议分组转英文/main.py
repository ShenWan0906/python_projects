import json
import os
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox
from translate import Translator


class TranslationApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("中文到英文翻译工具")
        self.root.geometry("600x250")

        self.setup_ui()

    def setup_ui(self):
        # 标题
        title_label = tk.Label(self.root, text="中文到英文JSON翻译工具",
                               font=("Arial", 16, "bold"))
        title_label.pack(pady=20)

        # 选择文件按钮
        select_btn = tk.Button(self.root, text="选择TXT文件",
                               command=self.select_file,
                               font=("Arial", 12),
                               width=20, height=2)
        select_btn.pack(pady=10)

        # 状态标签
        self.status_label = tk.Label(self.root, text="请选择要翻译的TXT文件",
                                     font=("Arial", 10))
        self.status_label.pack(pady=10)

        # 进度标签
        self.progress_label = tk.Label(self.root, text="", font=("Arial", 9))
        self.progress_label.pack(pady=5)

        # 翻译次数计数器
        self.translation_count = 0

    def translate_chinese_to_english(self, text):
        """将中文文本翻译成英文"""
        if not text or not isinstance(text, str):
            return text

        # 检查文本是否包含中文字符
        if not any('\u4e00' <= char <= '\u9fff' for char in text):
            return text

        try:
            # 使用translate库
            translator = Translator(to_lang="en", from_lang="zh")
            translation = translator.translate(text)

            # 更新进度显示
            self.translation_count += 1
            if self.translation_count % 10 == 0:
                self.progress_label.config(text=f"已翻译 {self.translation_count} 个字段...")
                self.root.update()

            return translation
        except Exception as e:
            print(f"翻译失败: {text}, 错误: {e}")
            return text

    def translate_json_data(self, data):
        """递归遍历JSON数据并翻译中文内容"""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                # 翻译键（如果键是中文）
                translated_key = self.translate_chinese_to_english(key)
                # 递归翻译值
                result[translated_key] = self.translate_json_data(value)
            return result
        elif isinstance(data, list):
            return [self.translate_json_data(item) for item in data]
        elif isinstance(data, str):
            return self.translate_chinese_to_english(data)
        else:
            return data

    def select_file(self):
        """选择文件对话框"""
        file_path = filedialog.askopenfilename(
            title="选择TXT文件",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )

        if file_path:
            self.process_file(file_path)

    def process_file(self, file_path):
        """处理选中的文件"""
        try:
            # 重置计数器
            self.translation_count = 0

            self.status_label.config(text="正在读取文件...")
            self.progress_label.config(text="")
            self.root.update()

            # 读取原始文件
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 解析JSON
            self.status_label.config(text="正在解析JSON...")
            self.root.update()

            data = json.loads(content)

            # 翻译JSON数据
            self.status_label.config(text="正在翻译内容...")
            self.progress_label.config(text="这可能需要一些时间，请稍候...")
            self.root.update()

            translated_data = self.translate_json_data(data)

            # 获取桌面路径
            desktop_path = Path.home() / "Desktop"
            file_name = os.path.basename(file_path)
            output_file = desktop_path / file_name.replace(".txt", "_english.txt")

            # 保存翻译后的JSON
            self.status_label.config(text="正在保存文件...")
            self.progress_label.config(text="")
            self.root.update()

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(translated_data, f, ensure_ascii=False, indent=2)

            # 显示完成消息
            self.status_label.config(text="翻译完成！")
            self.progress_label.config(text=f"文件已保存到: {output_file}")

            messagebox.showinfo("完成",
                                f"翻译完成！\n共翻译了 {self.translation_count} 个字段\n文件已保存到:\n{output_file}")

        except FileNotFoundError:
            messagebox.showerror("错误", f"找不到文件: {file_path}")
        except json.JSONDecodeError as e:
            messagebox.showerror("错误", f"JSON解析失败: {e}")
        except Exception as e:
            messagebox.showerror("错误", f"处理文件时发生错误: {e}")
        finally:
            # 重置状态
            self.status_label.config(text="请选择要翻译的TXT文件")
            self.progress_label.config(text="")

    def run(self):
        """运行应用程序"""
        self.root.mainloop()


if __name__ == "__main__":
    # 检查并安装所需的库
    try:
        from translate import Translator
    except ImportError:
        print("请先安装所需的库：")
        print("pip install translate")
        exit(1)

    app = TranslationApp()
    app.run()