package com.fishsun.bigdata;

public class HTMLPage {

  public static String getIndexPage() {
    return "<html>" +
            "<head>" +
            "<title>Spark SQL Executor</title>" +
            "<link href='static/bootstrap/css/bootstrap.min.css' rel='stylesheet'>" +
            "<script src='static/bootstrap/js/bootstrap.bundle.min.js'></script>" +
            "<style>" +
            "body, html { height: 100%; margin: 0; }" +
            ".container { height: 100vh; display: flex; flex-direction: column; padding: 20px; }" +
            "#sql-input { flex: 1; height: 200px; resize: none; }" +
            "#execute-btn, #cancel-btn { margin-top: 10px; }" +
            "#loading-spinner { display: none; text-align: center; }" +
            "#loading-spinner .spinner-border { width: 3rem; height: 3rem; }" +
            "#result { flex: 2; overflow: auto; margin-top: 10px; background: #f8f9fa; padding: 15px; border: 1px solid #ddd; }" +
            "table { margin-top: 10px; }" +
            "th { position: sticky; top: 0; background: #f8f9fa; }" +
            "th, td { white-space: nowrap; }" +
            "</style>" +
            "<script>" +
            "var controller;" +
            "var currentJobGroup = '';" +
            "function showLoadingSpinner() {" +
            "    document.getElementById('loading-spinner').style.display = 'block';" +
            "}" +
            "function hideLoadingSpinner() {" +
            "    document.getElementById('loading-spinner').style.display = 'none';" +
            "}" +
            "function executeSQL() {" +
            "    var sql = document.getElementById('sql-input').value.trim();" +
            "    if (!sql) {" +
            "        alert('请输入有效的 SQL 语句');" +
            "        return;" +
            "    }" +
            "    currentJobGroup = 'job-' + Math.random().toString(36).substring(2, 15);" +  // 生成唯一 jobId" +
            "    controller = new AbortController();" + // 初始化新的 AbortController" +
            "    showLoadingSpinner();" + // 显示加载动画" +
            "    fetch('/execute', {" +
            "        method: 'POST'," +
            "        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }," +
            "        body: 'sql=' + encodeURIComponent(sql) + '&jobGroup=' + currentJobGroup," +
            "        signal: controller.signal" + // 绑定 AbortController 信号" +
            "    }).then(response => response.json())" +
            "    .then(result => {" +
            "        if (result.error) {" +
            "            document.getElementById('result').innerHTML = '<div class=\"alert alert-danger\">' + result.error + '</div>';" +
            "        } else {" +
            "            renderTable(result.columns, result.data);" +
            "        }" +
            "    }).catch(error => {" +
            "        if (error.name === 'AbortError') {" +
            "            document.getElementById('result').innerHTML = '<div class=\"alert alert-warning\">SQL 执行已取消。</div>';" +
            "        } else {" +
            "            console.error('SQL 执行失败:', error);" +
            "            alert('SQL 执行失败: ' + error.message);" +
            "        }" +
            "    }).finally(() => {" +
            "        hideLoadingSpinner();" + // 隐藏加载动画" +
            "    });" +
            "}" +
            "function cancelSQL() {" +
            "    if (controller) {" +
            "        controller.abort();" +  // 取消前端请求" +
            "        fetch('/cancel', {" +  // 后端取消 Spark Job" +
            "            method: 'POST'," +
            "            headers: { 'Content-Type': 'application/x-www-form-urlencoded' }," +
            "            body: 'jobGroup=' + encodeURIComponent(currentJobGroup)" +
            "        });" +
            "    }" +
            "}" +
            "function renderTable(columns, data) {" +
            "    var html = '<h3 class=\"text-center\">SQL 执行结果</h3>';" +
            "    html += '<div class=\"table-responsive\"><table class=\"table table-bordered table-striped\"><thead><tr>';" +
            "    html += '<th>#</th>';" +
            "    columns.forEach(col => {" +
            "        html += '<th>' + col + '</th>';" +
            "    });" +
            "    html += '</tr></thead><tbody>';" +
            "    data.forEach((row, index) => {" +
            "        html += '<tr><td>' + (index + 1) + '</td>';" +
            "        row.forEach(cell => {" +
            "            html += '<td>' + cell + '</td>';" +
            "        });" +
            "        html += '</tr>';" +
            "    });" +
            "    html += '</tbody></table></div>';" +
            "    document.getElementById('result').innerHTML = html;" +
            "}" +
            "</script>" +
            "</head>" +
            "<body>" +
            "<div class='container'>" +
            "<h1 class='text-center mb-4'>Spark SQL Executor</h1>" +
            "<textarea id='sql-input' class='form-control' placeholder='输入您的 SQL 查询语句...'></textarea>" +
            "<button id='execute-btn' class='btn btn-primary w-100' onclick='executeSQL()'>执行 SQL</button>" +
            "<button id='cancel-btn' class='btn btn-danger w-100' onclick='cancelSQL()'>取消执行</button>" +  // 添加取消执行按钮" +
            "<div id='loading-spinner' class='mt-4'>" +
            "  <div class='spinner-border text-primary' role='status'>" +
            "    <span class='visually-hidden'>Loading...</span>" +
            "  </div>" +
            "</div>" +
            "<div id='result' class='card mt-4'>" +
            "<h4 class='text-center'>SQL 执行结果将在此显示...</h4>" +
            "</div>" +
            "</div>" +
            "</body>" +
            "</html>";
  }
}
