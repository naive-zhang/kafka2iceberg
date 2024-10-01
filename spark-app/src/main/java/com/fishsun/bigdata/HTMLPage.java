package com.fishsun.bigdata;

public class HTMLPage {

  public static String getIndexPage() {
    return "<!DOCTYPE html>\n" +
            "<html>\n" +
            "\n" +
            "<head>\n" +
            "    <meta charset='UTF-8'>\n" +
            "    <title>Spark SQL Executor Preview Edition</title>\n" +
            "    <link href='/static/bootstrap/css/bootstrap.min.css' rel='stylesheet'>\n" +
            "    <script src='/static/bootstrap/js/bootstrap.bundle.min.js'></script>\n" +
            "    <style>\n" +
            "        body,\n" +
            "        html {\n" +
            "            height: 100%;\n" +
            "            margin: 0;\n" +
            "        }\n" +
            "\n" +
            "        .container {\n" +
            "            height: 100vh;\n" +
            "            display: flex;\n" +
            "            flex-direction: column;\n" +
            "            padding: 20px;\n" +
            "        }\n" +
            "\n" +
            "        #sql-input {\n" +
            "            flex: 1;\n" +
            "            height: 200px;\n" +
            "            resize: none;\n" +
            "        }\n" +
            "\n" +
            "        #execute-btn,\n" +
            "        #execute-all-btn,\n" +
            "        #cancel-btn {\n" +
            "            margin-top: 10px;\n" +
            "        }\n" +
            "\n" +
            "        #loading-spinner {\n" +
            "            display: none;\n" +
            "            text-align: center;\n" +
            "        }\n" +
            "\n" +
            "        #loading-spinner .spinner-border {\n" +
            "            width: 3rem;\n" +
            "            height: 3rem;\n" +
            "        }\n" +
            "\n" +
            "        #result {\n" +
            "            flex: 2;\n" +
            "            overflow: auto;\n" +
            "            margin-top: 10px;\n" +
            "            background: #f8f9fa;\n" +
            "            padding: 15px;\n" +
            "            border: 1px solid #ddd;\n" +
            "        }\n" +
            "\n" +
            "        table {\n" +
            "            margin-top: 10px;\n" +
            "        }\n" +
            "\n" +
            "        th {\n" +
            "            position: sticky;\n" +
            "            top: 0;\n" +
            "            background: #f8f9fa;\n" +
            "        }\n" +
            "\n" +
            "        th,\n" +
            "        td {\n" +
            "            white-space: nowrap;\n" +
            "        }\n" +
            "    </style>\n" +
            "    <script>\n" +
            "        var controller;\n" +
            "        var currentJobGroup = '';\n" +
            "\n" +
            "        function showLoadingSpinner() {\n" +
            "            document.getElementById('loading-spinner').style.display = 'block';\n" +
            "        }\n" +
            "\n" +
            "        function hideLoadingSpinner() {\n" +
            "            document.getElementById('loading-spinner').style.display = 'none';\n" +
            "        }\n" +
            "\n" +
            "        function executeSQL() {\n" +
            "            var sql = document.getElementById('sql-input').value.trim();\n" +
            "            if (!sql) {\n" +
            "                alert('请输入有效的 SQL 语句');\n" +
            "                return;\n" +
            "            }\n" +
            "            currentJobGroup = 'job-' + Math.random().toString(36).substring(2, 15);  // 生成唯一 jobId\n" +
            "            controller = new AbortController(); // 初始化新的 AbortController\n" +
            "            showLoadingSpinner(); // 显示加载动画\n" +
            "            fetch('/execute', {\n" +
            "                method: 'POST',\n" +
            "                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },\n" +
            "                body: 'sql=' + encodeURIComponent(sql) + '&jobGroup=' + currentJobGroup,\n" +
            "                signal: controller.signal // 绑定 AbortController 信号\n" +
            "            }).then(response => response.json())\n" +
            "                .then(result => {\n" +
            "                    if (result.error) {\n" +
            "                        document.getElementById('result').innerHTML = '<div class=\"alert alert-danger\">' + result.error + '</div>';\n" +
            "                    } else {\n" +
            "                        // 使用 renderAllResults 来渲染结果\n" +
            "                        renderAllResults([{ command: '执行 SQL', data: result.data,columns: result.columns  }]);\n" +
            "                    }\n" +
            "                }).catch(error => {\n" +
            "                    if (error.name === 'AbortError') {\n" +
            "                        document.getElementById('result').innerHTML = '<div class=\"alert alert-warning\">SQL 执行已取消。</div>';\n" +
            "                    } else {\n" +
            "                        console.error('SQL 执行失败:', error);\n" +
            "                        alert('SQL 执行失败: ' + error.message);\n" +
            "                    }\n" +
            "                }).finally(() => {\n" +
            "                    hideLoadingSpinner();\n" +
            "                });\n" +
            "        }\n" +
            "\n" +
            "        function executeAll() {\n" +
            "            var sql = document.getElementById('sql-input').value.trim();\n" +
            "            if (!sql) {\n" +
            "                alert('请输入有效的 SQL 或 Shell 命令');\n" +
            "                return;\n" +
            "            }\n" +
            "            var commands = sql.split(';').map(cmd => cmd.trim()).filter(cmd => cmd.length > 0);\n" +
            "            var results = [];\n" +
            "            currentJobGroup = 'job-' + Math.random().toString(36).substring(2, 15);  // 生成唯一 jobId\n" +
            "            controller = new AbortController(); // 初始化新的 AbortController\n" +
            "            showLoadingSpinner(); // 显示加载动画\n" +
            "            executeNext(commands, 0, results);\n" +
            "        }\n" +
            "\n" +
            "        function executeNext(commands, index, results) {\n" +
            "            if (index >= commands.length) {\n" +
            "                console.log('index 达到结束的准则. '); " +
            "                console.log(index); " +
            "                console.log(commands); " +
            "                console.log(results); " +
            "                renderAllResults(results);\n" +
            "                hideLoadingSpinner();\n" +
            "                return;\n" +
            "            }\n" +
            "            var command = commands[index];\n" +
            "            if (command.startsWith('!')) {\n" +
            "                executeShellCommand(command.substring(1), results, () => executeNext(commands, index + 1, results));\n" +
            "            } else {\n" +
            "                executeSingleSQL(command, results, () => executeNext(commands, index + 1, results));\n" +
            "            }\n" +
            "        }\n" +
            "\n" +
            "        function executeSingleSQL(sql, results, callback) {\n" +
            "            fetch('/execute', {\n" +
            "                method: 'POST',\n" +
            "                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },\n" +
            "                body: 'sql=' + encodeURIComponent(sql) + '&jobGroup=' + currentJobGroup,\n" +
            "                signal: controller.signal\n" +
            "            }).then(response => response.json())\n" +
            "                .then(result => {\n" +
            "                    if (result.error) {\n" +
            "                        results.push({ command: sql, error: result.error });\n" +
            "                    } else {\n" +
            "                        results.push({ command: sql, data: result.data, columns:result.columns });\n" +
            "                    }\n" +
            "                }).catch(error => {\n" +
            "                    if (error.name === 'AbortError') {\n" +
            "                        results.push({ command: sql, error: 'SQL 执行已取消。' });\n" +
            "                    } else {\n" +
            "                        console.error('SQL 执行失败:', error);\n" +
            "                        results.push({ command: sql, error: 'SQL 执行失败: ' + error.message });\n" +
            "                    }\n" +
            "                }).finally(() => {\n" +
            "                    callback();\n" +
            "                });\n" +
            "        }\n" +
            "\n" +
            "        function executeShellCommand(cmd, results, callback) {\n" +
            "            fetch('/execute-shell', { // 假设有一个新的端点处理 Shell 命令\n" +
            "                method: 'POST',\n" +
            "                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },\n" +
            "                body: 'command=' + encodeURIComponent(cmd),\n" +
            "                signal: controller.signal\n" +
            "            }).then(response => response.json())\n" +
            "                .then(result => {\n" +
            "                    if (result.error) {\n" +
            "                        results.push({ command: '!' + cmd, error: result.error });\n" +
            "                    } else {\n" +
            "                        results.push({ command: '!' + cmd, data: [result.output], columns: ['output'] });\n" +
            "                    }\n" +
            "                }).catch(error => {\n" +
            "                    if (error.name === 'AbortError') {\n" +
            "                        results.push({ command: '!' + cmd, error: 'Shell 命令执行已取消。' });\n" +
            "                    } else {\n" +
            "                        console.error('Shell 命令执行失败:', error);\n" +
            "                        results.push({ command: '!' + cmd, error: 'Shell 命令执行失败: ' + error.message });\n" +
            "                    }\n" +
            "                }).finally(() => {\n" +
            "                    callback();\n" +
            "                });\n" +
            "        }\n" +
            "\n" +
            "        function renderAllResults(results) {\n" +
            "             console.log('开始渲染结果');\n" +
            "             console.log(results);\n" +
            "            var html = '<h3 class=\"text-center\">执行结果</h3>';\n" +
            "            for (var i = 0; i < results.length; i++) {\n" +
            "                 var result = results[i];" +
            "                if (result.error) {\n" +
            "                    html += '<div class=\"alert alert-danger\"><strong>' + result.command + '</strong>: ' + result.error + '</div>';\n" +
            "                } else {\n" +
            "                    if (result.data.length > 0 && typeof result.data[0] === 'string') {\n" +
            "                        // Shell 命令输出\n" +
            "                        html += '<div class=\"alert alert-success\"><strong>' + result.command + '</strong>:<pre>' + result.data.join('\\n') + '</pre></div>';\n" +
            "                    } else {\n" +
            "                        // SQL 结果\n" +
            "                        html += '<div class=\"mb-4\"><h5>' + result.command + '</h5>';\n" +
            "                        html += '<div class=\"table-responsive\"><table class=\"table table-bordered table-striped\"><thead><tr>';\n" +
            "                        html += '<th>#</th>';\n" +
            "                         console.log(result.columns);" +
            "                        result.columns.forEach(function (col) {\n" +
            "                            html += '<th>' + col + '</th>';\n" +
            "                        });\n" +
            "                        html += '</tr></thead><tbody>';\n" +
            "                        result.data.forEach(function (row, index) {\n" +
            "                            html += '<tr>';\n" +
            "                            html += '<td>' + (index + 1) + '</td>';\n" +
            "                            row.forEach(function (cell) {\n" +
            "                                html += '<td>' + cell + '</td>';\n" +
            "                            });\n" +
            "                            html += '</tr>';\n" +
            "                        });\n" +
            "                        html += '</tbody></table></div></div>';\n" +
            "                    }\n" +
            "                }\n" +
            "            };\n" +
            "            document.getElementById('result').innerHTML = html;\n" +
            "        }\n" +
            "\n" +
            "        function cancelSQL() {\n" +
            "            if (controller) {\n" +
            "                controller.abort();  // 取消前端请求\n" +
            "                fetch('/cancel', {  // 后端取消 Spark Job\n" +
            "                    method: 'POST',\n" +
            "                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },\n" +
            "                    body: 'jobGroup=' + encodeURIComponent(currentJobGroup)\n" +
            "                }).then(response => response.json())\n" +
            "                    .then(result => {\n" +
            "                        if (result.error) {\n" +
            "                            alert('取消失败: ' + result.error);\n" +
            "                        } else {\n" +
            "                            alert('取消成功');\n" +
            "                            document.getElementById('result').innerHTML = '<div class=\"alert alert-warning\">SQL 执行已取消。</div>';\n" +
            "                        }\n" +
            "                    }).catch(error => {\n" +
            "                        console.error('取消请求失败:', error);\n" +
            "                        alert('取消请求失败: ' + error.message);\n" +
            "                    });\n" +
            "            }\n" +
            "        }\n" +
            "\n" +
            "    </script>\n" +
            "</head>\n" +
            "\n" +
            "<body>\n" +
            "    <div class='container'>\n" +
            "        <h1 class='text-center mb-4'>Spark SQL Executor</h1>\n" +
            "        <textarea id='sql-input' class='form-control'\n" +
            "            placeholder='输入您的 SQL 查询语句或 Shell 命令，每条语句以 ; 分割，Shell 命令以 ! 开头...'></textarea>\n" +
            "        <button id='execute-btn' class='btn btn-primary w-100' onclick='executeSQL()'>执行 SQL</button>\n" +
            "        <button id='execute-all-btn' class='btn btn-success w-100' onclick='executeAll()'>执行所有</button>\n" +
            "        <button id='cancel-btn' class='btn btn-danger w-100' onclick='cancelSQL()'>取消执行</button> \n" +
            "        <div id='loading-spinner' class='mt-4'>\n" +
            "            <div class='spinner-border text-primary' role='status'>\n" +
            "                <span class='visually-hidden'>Loading...</span>\n" +
            "            </div>\n" +
            "        </div>\n" +
            "        <div id='result' class='card mt-4'>\n" +
            "            <h4 class='text-center'>执行结果将在此显示...</h4>\n" +
            "        </div>\n" +
            "    </div>\n" +
            "</body>\n" +
            "\n" +
            "</html>";
  }
}
