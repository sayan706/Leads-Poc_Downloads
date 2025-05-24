import express from "express";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
import { TextEncoder } from "util";
import multer from "multer";
import fs from "fs";
import os from "os";
import path from "path";
import duckdb from "duckdb";
import axios from "axios";
import { parse, format } from "fast-csv";
import { writeToStream } from "fast-csv";

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;
const upload = multer({ dest: "uploads/" });
const HATCH_API_KEY =
  process.env.HATCH_API_KEY ||
  "U2FsdGVkX1-z0zrPMSDgg-V3VhtHWjR8DJYlalXwM2XFGV0WLWcFIGqZ_qwbayZYXmFb1DXqpb_enKy2hyLTjQ";

// Init Supabase client once
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

function runQuery(connection, query) {
  return new Promise((resolve, reject) => {
    connection.all(query, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}
// const { writeToStream } = require("fast-csv");

// --- Helper to clean phone number ---
function cleanPhoneNumber(phone) {
  if (Array.isArray(phone) && phone.length > 0) phone = phone[0];
  if (typeof phone === "string") {
    return phone.replace(/[^\d+]/g, "");
  }
  return "Error";
}

// --- Helper to write, upload, and respond ---
async function exportCSVAndUpload(results, supabase, res, filePath) {
  const outPath = path.join(os.tmpdir(), `hatch_output_${Date.now()}.csv`);
  const writeStream = fs.createWriteStream(outPath);

  const safeResults = results.map((row) => {
    const { rn, ...rest } = row;
    return {
      ...rest,
      phonenumber: row.phonenumber
        ? `="${row.phonenumber}"` // Excel-safe to prevent scientific format
        : "",
      // ? `="${cleanPhoneNumber(row.phonenumber)}"` // Excel-safe and cleaned
      // : "",
    };
  });

  writeStream.on("finish", async () => {
    try {
      const fileBuffer = fs.readFileSync(outPath);
      const fileName = `hatch_result_${Date.now()}.csv`;

      const { error: uploadErr } = await supabase.storage
        .from("hatch-exports")
        .upload(fileName, fileBuffer, {
          contentType: "text/csv",
          upsert: true,
        });

      if (uploadErr) {
        console.error("Upload failed:", uploadErr.message);
        return res.status(500).send("Failed to upload result.");
      }

      const { data: publicData } = supabase.storage
        .from("hatch-exports")
        .getPublicUrl(fileName);

      const { data: listData } = await supabase.storage
        .from("hatch-exports")
        .list("", { limit: 100 });

      const sorted = listData
        .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
        .slice(3);

      for (const f of sorted) {
        await supabase.storage.from("hatch-exports").remove([f.name]);
      }

      const shouldCleanup = process.env.CLEANUP_FILES === "true";

      if (shouldCleanup) {
        // Delay to avoid EBUSY errors (especially on Windows)
        setTimeout(() => {
          try {
            fs.unlinkSync(filePath);
          } catch (e) {
            console.error("Failed to delete filePath:", e.message);
          }

          try {
            fs.unlinkSync(outPath);
          } catch (e) {
            console.error("Failed to delete outPath:", e.message);
          }
        }, 1000); // 1-second delay
      }
      res.redirect(publicData.publicUrl);

      // Optionally delete the file after some time
      setTimeout(async () => {
        const { error: deleteError } = await supabase.storage
          .from("hatch-exports")
          .remove([fileName]);

        if (deleteError) {
          console.error("Failed to delete file:", deleteError.message);
        } else {
          console.log(`File ${fileName} deleted from bucket.`);
        }
      }, 60000);
    } catch (e) {
      console.error("Error in upload process:", e);
      return res.status(500).send("Something went wrong.");
    }
  });

  writeToStream(writeStream, safeResults, {
    headers: true,
    quoteColumns: true,
  });
}

// --- Main Upload Route ---
app.post("/upload", upload.single("file"), async (req, res) => {
  const limit = parseInt(req.body.limit);
  if (!limit || !req.file) return res.status(400).send("Invalid input.");

  const filePath = req.file.path;
  const rows = [];

  fs.createReadStream(filePath)
    .pipe(parse({ headers: true }))
    .on("data", (row) => rows.push(row))
    .on("end", async () => {
      const db = new duckdb.Database(":memory:");
      const con = db.connect();

      await con.run(`
        CREATE TABLE uploaded AS 
        SELECT * 
        FROM read_csv_auto('${filePath.replace(/\\/g, "\\\\")}')
      `);

      const limited = await runQuery(
        con,
        `
        SELECT *
        FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY company ORDER BY linkedin) as rn
          FROM uploaded
        )
        WHERE rn <= ${limit};
      `
      );

      const results = [];
      for (const row of limited) {
        let phone = "Not Found";
        try {
          const response = await axios.post(
            "https://api.hatchhq.ai/v1/findPhone",
            { linkedinUrl: row.linkedin },
            { headers: { "x-api-key": process.env.HATCH_API_KEY } }
          );

          const rawPhone = response.data.phone;
          phone = cleanPhoneNumber(rawPhone || "Not Found");
        } catch (err) {
          console.error(`Error for ${row.linkedin}:`, err.message);
        }

        results.push({ ...row, phonenumber: phone });
      }

      await exportCSVAndUpload(results, supabase, res, filePath);
    });
});
app.get("/export-leads", async (req, res) => {
  const clientId = req.query.client_id;

  if (!clientId) {
    return res.status(400).json({ error: "Missing client_id" });
  }

  try {
    const { fileName, downloadUrl } = await exportLeads(clientId);

    // Redirect the user to the download URL
    res.redirect(downloadUrl);

    // Wait a short time to ensure download starts, then delete file
    setTimeout(async () => {
      const { error: deleteError } = await supabase.storage
        .from("leadexports")
        .remove([fileName]);

      if (deleteError) {
        console.error("Failed to delete file:", deleteError.message);
      } else {
        console.log(`File ${fileName} deleted from bucket.`);
      }
    }, 60000); // wait 5 seconds before deleting
  } catch (err) {
    console.error("Export failed:", err);
    return res.status(500).json({ error: "Export failed" });
  }
});

async function exportLeads(clientId) {
  const bucket = "leadexports";
  const timestamp = Date.now();
  const fileName = `Leads_${clientId}_${timestamp}.csv`;

  const pageSize = 1000;
  let page = 0;
  const encoder = new TextEncoder();
  const csvParts = [];
  let headersWritten = false;

  while (true) {
    const from = page * pageSize;
    const to = from + pageSize - 1;

    const { data, error } = await supabase
      .from("Leads")
      .select("*")
      .eq("client_id", clientId)
      .range(from, to);

    if (error) {
      throw new Error(`Error fetching leads: ${error.message}`);
    }

    if (!data || data.length === 0) break;

    if (!headersWritten) {
      const headerRow = Object.keys(data[0]).join(",") + "\n";
      csvParts.push(encoder.encode(headerRow));
      headersWritten = true;
    }

    for (const row of data) {
      const rowStr =
        Object.values(row)
          .map((val) => `"${String(val ?? "").replace(/"/g, '""')}"`)
          .join(",") + "\n";
      csvParts.push(encoder.encode(rowStr));
    }

    if (data.length < pageSize) break;
    page++;
  }

  if (csvParts.length === 0) {
    console.warn("No data found for client_id:", clientId);
    return;
  }

  const buffer = Buffer.concat(csvParts);
  console.log(`CSV ready: ${fileName} (${buffer.length} bytes)`);

  // Upload CSV
  const { error: uploadError } = await supabase.storage
    .from(bucket)
    .upload(fileName, buffer, {
      contentType: "text/csv",
    });

  if (uploadError) {
    throw new Error(`Upload failed: ${uploadError.message}`);
  }

  console.log("File uploaded:", fileName);

  // Get public URL (since you're using a public bucket)
  const { data: urlData, error: urlError } = supabase.storage
    .from(bucket)
    .getPublicUrl(fileName);

  if (urlError || !urlData?.publicUrl) {
    throw new Error("Could not get public URL");
  }

  console.log("Download URL:", urlData.publicUrl);

  return {
    fileName,
    downloadUrl: urlData.publicUrl,
  };

  // Optionally: Notify via email, webhook, or store URL in DB
}

app.get("/poc-level-exports", async (req, res) => {
  const clientId = req.query.client_id;

  if (!clientId) {
    return res.status(400).json({ error: "Missing client_id" });
  }

  try {
    const { fileName, downloadUrl } = await exportPocLevels(clientId);

    // Redirect the user to the download URL
    res.redirect(downloadUrl);

    // Wait a short time to ensure download starts, then delete file
    setTimeout(async () => {
      const { error: deleteError } = await supabase.storage
        .from("poclevelexports")
        .remove([fileName]);

      if (deleteError) {
        console.error("Failed to delete file:", deleteError.message);
      } else {
        console.log(`File ${fileName} deleted from poclevelexports bucket.`);
      }
    }, 60000); // wait 1 minute before deleting
  } catch (err) {
    console.error("POC Level export failed:", err);
    return res.status(500).json({ error: "POC Level export failed" });
  }
});

async function exportPocLevels(clientId) {
  const bucket = "poclevelexports";
  const timestamp = Date.now();
  const fileName = `POC_Levels_${clientId}_${timestamp}.csv`;

  const pageSize = 1000;
  let page = 0;
  const encoder = new TextEncoder();
  const csvParts = [];
  let headersWritten = false;

  while (true) {
    const from = page * pageSize;
    const to = from + pageSize - 1;

    const { data, error } = await supabase
      .from("POC-Data") // <- Make sure this is the correct table name
      .select("*")
      .eq("client_id", clientId)
      .range(from, to);

    if (error) {
      throw new Error(`Error fetching POC levels: ${error.message}`);
    }

    if (!data || data.length === 0) break;

    if (!headersWritten) {
      const headerRow = Object.keys(data[0]).join(",") + "\n";
      csvParts.push(encoder.encode(headerRow));
      headersWritten = true;
    }

    for (const row of data) {
      const rowStr =
        Object.values(row)
          .map((val) => `"${String(val ?? "").replace(/"/g, '""')}"`)
          .join(",") + "\n";
      csvParts.push(encoder.encode(rowStr));
    }

    if (data.length < pageSize) break;
    page++;
  }

  if (csvParts.length === 0) {
    throw new Error(`No data found for client_id: ${clientId}`);
  }

  const buffer = Buffer.concat(csvParts);
  console.log(`POC CSV ready: ${fileName} (${buffer.length} bytes)`);

  const { error: uploadError } = await supabase.storage
    .from(bucket)
    .upload(fileName, buffer, {
      contentType: "text/csv",
    });

  if (uploadError) {
    throw new Error(`POC Upload failed: ${uploadError.message}`);
  }

  console.log("POC File uploaded:", fileName);

  const { data: urlData, error: urlError } = supabase.storage
    .from(bucket)
    .getPublicUrl(fileName);

  if (urlError || !urlData?.publicUrl) {
    throw new Error("Could not get POC public URL");
  }

  console.log("POC Download URL:", urlData.publicUrl);

  return {
    fileName,
    downloadUrl: urlData.publicUrl,
  };
}

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
