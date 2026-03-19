#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 AmazingData开发手册.txt 转为 Markdown"""

import re
import sys
from pathlib import Path

def main():
    src = Path(__file__).parent / "AmazingData开发手册.txt"
    dst = Path(__file__).parent / "AmazingData开发手册.md"
    
    text = src.read_text(encoding="utf-8")
    lines = text.split("\n")
    out = []
    i = 0
    in_code_block = False
    code_buffer = []
    in_table = False
    table_rows = []
    
    # 页眉/页脚
    page_marker = re.compile(r"^=+\s*$")
    page_num = re.compile(r"^第\s*\d+\s*页\s*$")
    repeated_header = "中国银河证券星耀数智服务平台金融资讯数据说明"
    
    def flush_code():
        nonlocal code_buffer, in_code_block
        if code_buffer:
            code_lines = []
            for line in code_buffer:
                # 提取 |  | xxx |  | 中的 xxx
                m = re.match(r"^\|?\s*\|?\s*(.*?)\s*\|?\s*$", line)
                if m:
                    content = m.group(1).strip()
                    content = re.sub(r"\s*\|\s*$", "", content)  # 去掉行尾的 |
                    # 修复常见乱码
                    content = content.replace("mport ", "import ").replace("iimport ", "import ")
                    content = content.replace(" nfo_", " info_").replace(" oday ", " today ")
                    content = content.replace("i i t ", "").replace("nfo_data", "info_data")
                    if content and not content.replace("|", "").replace("-", "").replace("+", "").strip() == "":
                        code_lines.append(content)
            if code_lines:
                out.append("```python")
                out.extend(code_lines)
                out.append("```")
                out.append("")
            code_buffer = []
        in_code_block = False
    
    def flush_table():
        nonlocal table_rows, in_table
        if not table_rows:
            in_table = False
            return
        # 收集所有行（含续行），只取非空单元格
        raw_rows = []
        for row in table_rows:
            cells = [c.strip() for c in row if c.strip() and not re.match(r"^[\s\-+|]+$", c.strip())]
            meaningful = [c for c in cells if len(c) > 1 or (c.isalnum() and c)]
            if not meaningful:
                continue
            raw_rows.append(meaningful)
        if not raw_rows:
            table_rows = []
            in_table = False
            return
        # 续行合并：仅 1～2 个单元格的行视为上一行的延续，拼到上一行对应列
        merged = []
        for row in raw_rows:
            if not merged:
                merged.append(row.copy())
                continue
            prev = merged[-1]
            nprev = len(prev)
            # 续行：当前行单元格很少，且上一行列数>=3
            if len(row) <= 2 and nprev >= 3:
                if len(row) == 1:
                    # 1 格：接到上一行倒数第二列（说明等长文本列常折行）
                    idx = max(0, nprev - 2)
                    prev[idx] = (prev[idx] or "") + (row[0] or "")
                else:
                    # 2 格：接到上一行最后两列
                    for k, cell in enumerate(row):
                        j = nprev - 2 + k
                        if j >= 0 and j < nprev:
                            prev[j] = (prev[j] or "") + (cell or "")
            else:
                merged.append(row.copy())
        # 输出：单列改为列表，多列为表格
        if merged:
            ncol = len(merged[0])
            out.append("")
            if ncol == 1:
                for row in merged:
                    cell = (row + [""])[0].strip()
                    if cell:
                        out.append("- " + cell)
            else:
                for idx, row in enumerate(merged):
                    if idx == 0:
                        out.append("| " + " | ".join(row[:ncol]) + " |")
                        if len(merged) > 1:
                            out.append("|" + " --- |" * ncol)
                    else:
                        pad = (row + [""] * ncol)[:ncol]
                        out.append("| " + " | ".join(pad) + " |")
            out.append("")
        table_rows = []
        in_table = False
    
    def is_table_separator(line):
        stripped = line.strip()
        if not stripped:
            return False
        return re.match(r"^[\+\-\|\s]+$", stripped) and "|" not in stripped.replace("|", "") or (stripped.startswith("+") and "-" in stripped)
    
    def parse_table_line(line):
        """从 | a | b | c | 解析出 [a, b, c]，保留所有非空单元格（含续行仅 1 格）"""
        if "|" not in line:
            return None
        parts = [p.strip() for p in line.split("|")]
        # 去掉纯分隔符（仅 - + 空格）
        parts = [p for p in parts if p and not re.match(r"^[\s\-+]+$", p)]
        return parts if parts else None
    
    def is_code_line(line):
        s = line.strip()
        if "|" in s:
            inner = re.sub(r"^\|?\s*\|?\s*", "", s)
            inner = re.sub(r"\s*\|?\s*\|?\s*$", "", inner).strip()
            return any(k in inner for k in ("import ", "ad.login", "ad.BaseData", "ad.InfoData", "def on", "def ", "ad.SubscribeData", "ad.constant", "sub_data.", "base_data_object.", "info_data_object.", "get_code_list", "get_calendar", "register(", ".run()", "password=", "username="))
        return False
    
    def section_level(line):
        """返回 (level, title) 或 None。如 3.5.2.1 标题 -> (5, '3.5.2.1 标题')"""
        stripped = line.strip()
        m = re.match(r"^(\d+(?:\.\d+)*)\s+(.+)$", stripped)
        if not m:
            return None
        num_part = m.group(1)
        title = m.group(2).strip()
        if not title:
            return None
        dots = num_part.count(".")
        level = min(6, 1 + dots)
        return (level, f"{num_part} {title}")
    
    while i < len(lines):
        line = lines[i]
        orig = line
        
        # 跳过页分隔
        if page_marker.match(line.strip()) and i + 2 < len(lines) and page_num.match(lines[i+1].strip()):
            i += 3
            if i < len(lines) and lines[i].strip() == repeated_header:
                i += 1
            continue
        
        if line.strip() == repeated_header:
            i += 1
            continue
        
        # 单独一行的数字（页码）
        if re.match(r"^\s*\d+\s*$", line) and (not out or out[-1].strip() == "" or out[-1].startswith("```")):
            i += 1
            continue
        
        # 检测小节标题（仅数字. 开头的独立行）
        sec = section_level(line)
        if sec and (not out or out[-1].strip() == "" or out[-1].startswith("#") or out[-1].startswith("|") or out[-1].startswith("```")):
            flush_code()
            flush_table()
            level, title = sec
            out.append("#" * level + " " + title)
            out.append("")
            i += 1
            continue
        
        # 代码块：连续多行 |  | code |  |
        if "|" in line and is_code_line(line):
            if not in_code_block:
                flush_table()
                in_code_block = True
                code_buffer = []
            code_buffer.append(line)
            i += 1
            continue
        else:
            if in_code_block and line.strip() == "":
                code_buffer.append(line)
                i += 1
                continue
            if in_code_block and "|" in line and not is_code_line(line):
                # 可能是表格行混入，先 flush 代码
                flush_code()
        
        if in_code_block and (line.strip() == "" or (not line.strip().startswith("|") and line.strip())):
            flush_code()
        
        # 表格行
        if line.strip().startswith("|") and not is_code_line(line):
            parsed = parse_table_line(line)
            if parsed and not is_table_separator(line):
                if not in_table:
                    flush_code()
                in_table = True
                table_rows.append(parsed)
                i += 1
                continue
            if is_table_separator(line):
                i += 1
                continue
        
        if in_table and line.strip() == "":
            flush_table()
        
        # 仅包含 + - 的行（表格边框）跳过
        if re.match(r"^[\s\|+\-]+$", line) and len(line.strip()) > 4:
            i += 1
            continue
        
        # 普通段落
        flush_code()
        if in_table:
            flush_table()
        
        # 保留空行（适度）
        if line.strip() == "":
            if out and out[-1].strip() != "":
                out.append("")
        else:
            # 去掉行尾的 ........ 页码
            cleaned = line.rstrip()
            cleaned = re.sub(r"\s*\.{2,}\s*\d+\s*$", "", cleaned)
            cleaned = re.sub(r"\s+\.{2,}\s*$", "", cleaned)
            if cleaned.strip():
                out.append(cleaned)
        i += 1
    
    flush_code()
    flush_table()
    
    # 合并多余空行
    result = []
    prev_blank = False
    for line in out:
        is_blank = line.strip() == ""
        if is_blank:
            if not prev_blank:
                result.append("")
            prev_blank = True
        else:
            result.append(line)
            prev_blank = False
    
    md_text = "\n".join(result).strip() + "\n"
    
    # 去掉开头重复的标题块，从「1. 版本说明」开始
    lines_out = result
    start_idx = 0
    for j, line in enumerate(lines_out):
        s = line.strip()
        if re.match(r"^1\.\s+版本说明\s*$", s):
            start_idx = j
            break
    if start_idx > 0:
        result = lines_out[start_idx:]
    
    # 再次合并多余空行
    final = []
    prev_blank = False
    for line in result:
        is_blank = line.strip() == ""
        if is_blank:
            if not prev_blank:
                final.append("")
            prev_blank = True
        else:
            final.append(line)
            prev_blank = False
    
    md_text = "\n".join(final).strip() + "\n"
    
    # 添加文档头
    title_block = """# 中国银河证券星耀数智 AmazingData 开发手册

中国银河证券星耀数智 · AmazingData 开发手册

---

"""
    md_text = title_block + md_text
    
    dst.write_text(md_text, encoding="utf-8")
    print(f"已生成: {dst}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
