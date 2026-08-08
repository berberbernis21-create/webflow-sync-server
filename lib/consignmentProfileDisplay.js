/**
 * Compact color-coded consignor / submission profile for internal email + PDF.
 */

export const PROFILE_GROUPS = [
  {
    key: "contact",
    title: "Contact",
    color: "#1a3c34",
    bg: "#e7f0ed",
    border: "#9cb8af",
    fields: ["name", "email", "phone"],
  },
  {
    key: "location",
    title: "Location",
    color: "#1e3a5f",
    bg: "#e8eef7",
    border: "#9db0cc",
    fields: ["address", "itemLocation", "pickup"],
  },
  {
    key: "submission",
    title: "Submission",
    color: "#6b4f1d",
    bg: "#f7f0e2",
    border: "#d2b98a",
    fields: ["preferredType", "source", "category", "submitted"],
  },
  {
    key: "counts",
    title: "Counts",
    color: "#5b2c6f",
    bg: "#f3eaf7",
    border: "#c9a7d9",
    fields: ["items", "photos"],
  },
];

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/**
 * @param {Record<string, string>} values keyed by field id
 */
export function buildConsignorProfileTableHtml(values) {
  const byId = values || {};
  const cells = [];
  for (const group of PROFILE_GROUPS) {
    for (const fieldId of group.fields) {
      if (!(fieldId in byId)) continue;
      cells.push({
        group,
        fieldId,
        label: fieldLabel(fieldId),
        value: byId[fieldId] ?? "Not provided",
      });
    }
  }

  let html = `<table style="border-collapse:separate;border-spacing:6px;width:100%;max-width:720px;font-size:12px;">`;
  for (let i = 0; i < cells.length; i += 2) {
    const left = cells[i];
    const right = cells[i + 1];
    html += `<tr>`;
    html += profileCellHtml(left);
    html += right
      ? profileCellHtml(right)
      : `<td style="width:50%;padding:0;"></td>`;
    html += `</tr>`;
  }
  html += `</table>`;

  // Legend
  html += `<p style="margin:4px 0 0;font-size:11px;color:#777;">`;
  html += PROFILE_GROUPS.map(
    (g) =>
      `<span style="display:inline-block;margin-right:10px;"><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:${g.bg};border:1px solid ${g.border};margin-right:4px;vertical-align:middle;"></span>${escapeHtml(g.title)}</span>`
  ).join("");
  html += `</p>`;
  return html;
}

function fieldLabel(fieldId) {
  const map = {
    name: "Name",
    email: "Email",
    phone: "Phone",
    address: "Address",
    itemLocation: "Item location",
    pickup: "Pickup / delivery",
    preferredType: "Preferred type",
    source: "Source",
    category: "Category",
    submitted: "Submitted",
    items: "Items",
    photos: "Photos",
  };
  return map[fieldId] || fieldId;
}

function profileCellHtml(cell) {
  const g = cell.group;
  return [
    `<td style="width:50%;vertical-align:top;padding:0;">`,
    `<div style="border:1px solid ${g.border};border-left:4px solid ${g.color};background:${g.bg};border-radius:4px;padding:7px 9px;min-height:42px;">`,
    `<div style="font-size:10px;font-weight:700;letter-spacing:0.04em;text-transform:uppercase;color:${g.color};margin:0 0 2px;">${escapeHtml(cell.label)}</div>`,
    `<div style="font-size:12px;color:#222;line-height:1.35;word-break:break-word;">${escapeHtml(cell.value)}</div>`,
    `</div></td>`,
  ].join("");
}

/**
 * Draw a compact 2-column color-coded profile table in PDFKit.
 */
export function drawConsignorProfileTable(doc, values, { margin, contentWidth }) {
  const byId = values || {};
  const cells = [];
  for (const group of PROFILE_GROUPS) {
    for (const fieldId of group.fields) {
      if (!(fieldId in byId)) continue;
      cells.push({
        group,
        label: fieldLabel(fieldId),
        value: String(byId[fieldId] ?? "Not provided"),
      });
    }
  }

  const gap = 8;
  const colW = (contentWidth - gap) / 2;
  const padX = 6;
  const padY = 5;

  for (let i = 0; i < cells.length; i += 2) {
    const left = cells[i];
    const right = cells[i + 1];
    const leftH = measureCellHeight(doc, left, colW - padX * 2);
    const rightH = right ? measureCellHeight(doc, right, colW - padX * 2) : 0;
    const rowH = Math.max(leftH, rightH, 28);

    if (doc.y + rowH + 6 > doc.page.height - margin - 40) {
      doc.addPage();
      doc.y = margin;
    }

    const y = doc.y;
    drawProfileCell(doc, left, margin, y, colW, rowH, padX, padY);
    if (right) {
      drawProfileCell(doc, right, margin + colW + gap, y, colW, rowH, padX, padY);
    }
    doc.y = y + rowH + 6;
  }

  // Legend
  doc.font("Helvetica").fontSize(7.5).fillColor("#777");
  let legendX = margin;
  const legendY = doc.y;
  for (const g of PROFILE_GROUPS) {
    doc.rect(legendX, legendY + 1, 7, 7).fillAndStroke(g.bg, g.border);
    doc.fillColor("#555").text(g.title, legendX + 10, legendY, { lineBreak: false });
    legendX += 10 + doc.widthOfString(g.title) + 12;
  }
  doc.y = legendY + 14;
}

function measureCellHeight(doc, cell, textWidth) {
  doc.font("Helvetica-Bold").fontSize(8);
  const labelH = doc.heightOfString(cell.label.toUpperCase(), { width: textWidth });
  doc.font("Helvetica").fontSize(9);
  const valueH = doc.heightOfString(cell.value, { width: textWidth });
  return 5 + labelH + 2 + valueH + 5;
}

function drawProfileCell(doc, cell, x, y, w, h, padX, padY) {
  const g = cell.group;
  const savedY = doc.y;
  doc.rect(x, y, w, h).fill(g.bg);
  doc.rect(x, y, 3.5, h).fill(g.color);
  doc.rect(x, y, w, h).strokeColor(g.border).lineWidth(0.6).stroke();
  doc.font("Helvetica-Bold").fontSize(8).fillColor(g.color);
  doc.text(cell.label.toUpperCase(), x + padX + 2, y + padY, {
    width: w - padX * 2 - 2,
    lineBreak: false,
  });
  doc.font("Helvetica").fontSize(9).fillColor("#222");
  doc.text(cell.value, x + padX + 2, y + padY + 11, {
    width: w - padX * 2 - 2,
    height: h - padY - 14,
    ellipsis: true,
  });
  doc.y = savedY;
}
