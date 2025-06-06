const form = document.getElementById('query-form');
const resultEl = document.getElementById('result');

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  const query = document.getElementById('query').value;
  resultEl.textContent = 'Running...';
  try {
    const resp = await fetch(`http://127.0.0.1:8005/query?query=${encodeURIComponent(query)}`);
    if (!resp.ok) {
      const text = await resp.text();
      resultEl.textContent = 'Error: ' + resp.status + '\n' + text;
      return;
    }
    const json = await resp.json();
    resultEl.textContent = JSON.stringify(json, null, 2);
  } catch (err) {
    resultEl.textContent = 'Network error: ' + err;
  }
});
