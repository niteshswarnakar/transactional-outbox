const REFRESH_MS = 2000;

const ordersBody = document.getElementById("orders");
const kafkaBody = document.getElementById("kafka-orders");
const countOrders = document.getElementById("count-orders");
const countKafka = document.getElementById("count-kafka");
const syncPill = document.getElementById("sync");
const updated = document.getElementById("updated");
const form = document.getElementById("order-form");
const formMsg = document.getElementById("form-msg");
const nameInput = document.getElementById("name");
const priceInput = document.getElementById("price");

async function getJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} returned ${res.status}`);
  return res.json();
}

function cell(text, className) {
  const td = document.createElement("td");
  td.textContent = text;
  if (className) td.className = className;
  return td;
}

function tag(text, className) {
  const td = document.createElement("td");
  const span = document.createElement("span");
  span.className = `tag ${className}`;
  span.textContent = text;
  td.appendChild(span);
  return td;
}

function emptyRow(colspan, text) {
  const tr = document.createElement("tr");
  const td = cell(text, "empty");
  td.colSpan = colspan;
  tr.appendChild(td);
  return tr;
}

function renderOrders(orders, deliveredIds) {
  ordersBody.replaceChildren();
  if (orders.length === 0) {
    ordersBody.appendChild(emptyRow(4, "No orders yet"));
    return;
  }
  for (const o of orders) {
    const tr = document.createElement("tr");
    tr.append(cell(o.id), cell(o.name), cell(o.price.toFixed(2), "num"));
    tr.appendChild(deliveredIds.has(o.id) ? tag("delivered", "ok") : tag("pending", "pending"));
    ordersBody.appendChild(tr);
  }
}

function renderKafka(orders) {
  kafkaBody.replaceChildren();
  if (orders.length === 0) {
    kafkaBody.appendChild(emptyRow(3, "Nothing consumed yet"));
    return;
  }
  for (const o of orders) {
    const tr = document.createElement("tr");
    tr.append(cell(o.id), cell(o.name), cell(o.price.toFixed(2), "num"));
    kafkaBody.appendChild(tr);
  }
}

function setSync(text, className) {
  syncPill.textContent = text;
  syncPill.className = `pill ${className}`;
}

async function refresh() {
  try {
    const [orders, kafkaOrders] = await Promise.all([
      getJSON("/orders"),
      getJSON("/kafka-orders"),
    ]);

    const deliveredIds = new Set(kafkaOrders.map((o) => o.id));
    renderOrders(orders, deliveredIds);
    renderKafka(kafkaOrders);
    countOrders.textContent = orders.length;
    countKafka.textContent = kafkaOrders.length;

    const pending = orders.filter((o) => !deliveredIds.has(o.id)).length;
    if (pending === 0) {
      setSync("In sync", "ok");
    } else {
      setSync(`${pending} pending in Kafka`, "warn");
    }
    updated.textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (err) {
    setSync("Cannot reach API", "err");
    updated.textContent = err.message;
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = form.querySelector("button");
  button.disabled = true;
  formMsg.className = "msg";
  formMsg.textContent = "";

  try {
    const res = await fetch("/orders", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: nameInput.value.trim(),
        price: parseFloat(priceInput.value),
      }),
    });
    const body = await res.json();
    if (!res.ok) throw new Error(body.error || `request failed (${res.status})`);

    formMsg.textContent = `Created order #${body.id}`;
    form.reset();
    nameInput.focus();
    await refresh();
  } catch (err) {
    formMsg.className = "msg error";
    formMsg.textContent = err.message;
  } finally {
    button.disabled = false;
  }
});

refresh();
setInterval(refresh, REFRESH_MS);
