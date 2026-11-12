import crypto from "crypto";

function expectedStaffCode() {
  return String(process.env.FREIGHT_STAFF_CODE || "2012").trim();
}

/** Store-employee code for in-store sales on the delivery calculator. */
export function isValidStaffCode(code) {
  const entered = Buffer.from(String(code ?? "").trim());
  const expected = Buffer.from(expectedStaffCode());
  if (!entered.length || entered.length !== expected.length) return false;
  return crypto.timingSafeEqual(entered, expected);
}
