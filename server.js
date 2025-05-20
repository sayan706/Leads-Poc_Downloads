import express from "express";
import cors from "cors";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
import { TextEncoder } from "util";

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.options("*", cors());

// Init Supabase client once
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

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
