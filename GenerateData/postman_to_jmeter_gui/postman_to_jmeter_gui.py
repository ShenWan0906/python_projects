import json
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
import tkinter as tk
from tkinter import filedialog, messagebox

def create_string_prop(name, value):
    prop = ET.Element('stringProp', name=name)
    prop.text = value
    return prop

def create_http_sampler(item):
    request = item['request']
    name = item.get('name', 'HTTP Request')
    method = request.get('method', 'GET')

    # 兼容 url 字段是字符串或 dict 的情况
    url_raw = ''
    if isinstance(request['url'], dict):
        url_raw = request['url'].get('raw', '')
    else:
        url_raw = request['url']

    parsed_url = urlparse(url_raw)
    domain = parsed_url.hostname or ""
    port = str(parsed_url.port) if parsed_url.port else ""
    path = parsed_url.path or "/"
    protocol = parsed_url.scheme or "http"

    sampler = ET.Element('HTTPSamplerProxy', {
        "guiclass": "HttpTestSampleGui",
        "testclass": "HTTPSamplerProxy",
        "testname": name,
        "enabled": "true"
    })

    sampler.append(create_string_prop("HTTPSampler.domain", domain))
    sampler.append(create_string_prop("HTTPSampler.port", port))
    sampler.append(create_string_prop("HTTPSampler.protocol", protocol))
    sampler.append(create_string_prop("HTTPSampler.path", path))
    sampler.append(create_string_prop("HTTPSampler.method", method))
    sampler.append(create_string_prop("HTTPSampler.contentEncoding", "utf-8"))
    sampler.append(create_string_prop("HTTPSampler.follow_redirects", "true"))
    sampler.append(create_string_prop("HTTPSampler.auto_redirects", "false"))
    sampler.append(create_string_prop("HTTPSampler.use_keepalive", "true"))
    sampler.append(create_string_prop("HTTPSampler.DO_MULTIPART_POST", "false"))
    sampler.append(create_string_prop("HTTPSampler.monitor", "false"))

    if 'body' in request and request['body'].get('raw'):
        sampler.append(create_string_prop("HTTPSampler.postBodyRaw", "true"))
        element_prop = ET.Element("elementProp", {
            "name": "HTTPsampler.Arguments",
            "elementType": "Arguments"
        })
        collection_prop = ET.SubElement(element_prop, "collectionProp", name="Arguments.arguments")
        arg = ET.SubElement(collection_prop, "elementProp", {
            "name": "",
            "elementType": "HTTPArgument"
        })
        arg.append(create_string_prop("Argument.value", request['body']['raw']))
        arg.append(create_string_prop("Argument.metadata", "="))
        sampler.append(element_prop)

    # Headers
    if 'header' in request and request['header']:
        header_mgr = ET.Element('HeaderManager', {
            "guiclass": "HeaderPanel",
            "testclass": "HeaderManager",
            "testname": "HTTP Header Manager",
            "enabled": "true"
        })
        headers_collection = ET.SubElement(header_mgr, "collectionProp", name="HeaderManager.headers")
        for h in request['header']:
            header_element = ET.SubElement(headers_collection, "elementProp", name="", elementType="Header")
            header_name = ET.SubElement(header_element, "stringProp", name="Header.name")
            header_name.text = h.get('key', '')
            header_value = ET.SubElement(header_element, "stringProp", name="Header.value")
            header_value.text = h.get('value', '')
        return sampler, header_mgr
    else:
        return sampler, None

def postman_to_jmeter(postman_path, output_path):
    with open(postman_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    test_plan = ET.Element("jmeterTestPlan", version="1.2", properties="5.0", jmeter="5.5")
    root_hash_tree = ET.SubElement(test_plan, "hashTree")

    test_plan_elem = ET.SubElement(root_hash_tree, "TestPlan", {
        "guiclass": "TestPlanGui",
        "testclass": "TestPlan",
        "testname": "Postman Imported Test Plan",
        "enabled": "true"
    })
    test_plan_elem.append(create_string_prop("TestPlan.comments", "Generated from Postman Collection"))
    test_plan_elem.append(create_string_prop("TestPlan.user_define_classpath", ""))

    test_plan_hash_tree = ET.SubElement(root_hash_tree, "hashTree")

    thread_group = ET.SubElement(test_plan_hash_tree, "ThreadGroup", {
        "guiclass": "ThreadGroupGui",
        "testclass": "ThreadGroup",
        "testname": "Thread Group",
        "enabled": "true"
    })
    thread_group.append(create_string_prop("ThreadGroup.num_threads", "1"))
    thread_group.append(create_string_prop("ThreadGroup.ramp_time", "1"))
    thread_group.append(create_string_prop("ThreadGroup.scheduler", "false"))
    thread_group.append(create_string_prop("ThreadGroup.duration", ""))

    thread_group_hash_tree = ET.SubElement(test_plan_hash_tree, "hashTree")

    def parse_items(items, parent_hash_tree):
        for item in items:
            if 'request' in item:
                sampler, header_mgr = create_http_sampler(item)
                parent_hash_tree.append(sampler)
                parent_hash_tree.append(ET.Element("hashTree"))
                if header_mgr:
                    parent_hash_tree.append(header_mgr)
                    parent_hash_tree.append(ET.Element("hashTree"))
            elif 'item' in item:
                parse_items(item['item'], parent_hash_tree)

    parse_items(data.get('item', []), thread_group_hash_tree)

    tree = ET.ElementTree(test_plan)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)

def select_file():
    file_path = filedialog.askopenfilename(title="选择 Postman Collection JSON 文件", filetypes=[("JSON 文件", "*.json")])
    if not file_path:
        return
    output_path = filedialog.asksaveasfilename(title="保存 JMeter 脚本为", defaultextension=".jmx", filetypes=[("JMeter 脚本", "*.jmx")])
    if not output_path:
        return
    try:
        postman_to_jmeter(file_path, output_path)
        messagebox.showinfo("成功", f"JMeter 脚本已生成:\n{output_path}")
    except Exception as e:
        messagebox.showerror("错误", f"生成失败:\n{str(e)}")

def main():
    root = tk.Tk()
    root.title("Postman to JMeter Converter")
    root.geometry("400x150")
    btn = tk.Button(root, text="选择 Postman JSON 文件", command=select_file, height=2, width=25)
    btn.pack(expand=True)
    root.mainloop()

if __name__ == "__main__":
    main()
