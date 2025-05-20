## window开发者执行
```bash
git config --global core.autocrlf true    # 提交时转 CRLF 为 LF，检出时转 LF 为 CRLF
```

## mac开发者执行
```bash
git config --global core.autocrlf input   # 提交时转 CRLF 为 LF，检出时不转换
```

## 格式化
```bash
gofumpt -l -w .
```
```bash
go install mvdan.cc/gofumpt@latest
```
