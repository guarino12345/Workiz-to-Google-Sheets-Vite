import { MongoClient, ObjectId } from "mongodb";
import { google } from "googleapis";
import { formatInTimeZone } from "date-fns-tz";

const MONGODB_URI = process.env.MONGODB_URI;

if (!MONGODB_URI) {
  console.error("MONGODB_URI is not defined in environment variables");
  process.exit(1);
}

let db;

async function connectToMongoDB() {
  try {
    const client = await MongoClient.connect(MONGODB_URI);
    db = client.db();
    console.log("Connected to MongoDB Atlas");
  } catch (error) {
    console.error("Error connecting to MongoDB:", error);
    throw error;
  }
}

// Helper function to check if it's time to sync based on frequency and time
function shouldSyncNow(account) {
  if (!account.syncEnabled) return false;

  const now = new Date();
  const currentHour = now.getHours();
  const currentMinute = now.getMinutes();

  // Parse sync time (HH:MM format)
  const [syncHour, syncMinute] = account.syncTime.split(":").map(Number);

  // Check if current time matches sync time (within 5 minutes window)
  const timeDiff = Math.abs(
    currentHour * 60 + currentMinute - (syncHour * 60 + syncMinute)
  );
  if (timeDiff > 5) return false;

  // Check frequency
  switch (account.syncFrequency) {
    case "daily":
      return true; // Run every day at the specified time

    case "weekly":
      // Run once per week on the same day of week
      const lastSync = account.lastSyncDate
        ? new Date(account.lastSyncDate)
        : null;
      if (!lastSync) return true;
      const daysSinceLastSync = Math.floor(
        (now - lastSync) / (1000 * 60 * 60 * 24)
      );
      return daysSinceLastSync >= 7;

    case "monthly":
      // Run once per month on the same day
      if (!lastSync) return true;
      const monthsSinceLastSync =
        (now.getFullYear() - lastSync.getFullYear()) * 12 +
        (now.getMonth() - lastSync.getMonth());
      return monthsSinceLastSync >= 1;

    case "custom":
      // For custom, we'll use a 24-hour interval
      if (!lastSync) return true;
      const hoursSinceLastSync = (now - lastSync) / (1000 * 60 * 60);
      return hoursSinceLastSync >= 24;

    default:
      return false;
  }
}

// Helper function to calculate next sync date
function calculateNextSyncDate(account) {
  const now = new Date();
  const [syncHour, syncMinute] = account.syncTime.split(":").map(Number);

  switch (account.syncFrequency) {
    case "daily":
      const tomorrow = new Date(now);
      tomorrow.setDate(tomorrow.getDate() + 1);
      tomorrow.setHours(syncHour, syncMinute, 0, 0);
      return tomorrow;

    case "weekly":
      const nextWeek = new Date(now);
      nextWeek.setDate(nextWeek.getDate() + 7);
      nextWeek.setHours(syncHour, syncMinute, 0, 0);
      return nextWeek;

    case "monthly":
      const nextMonth = new Date(now);
      nextMonth.setMonth(nextMonth.getMonth() + 1);
      nextMonth.setHours(syncHour, syncMinute, 0, 0);
      return nextMonth;

    case "custom":
      const nextDay = new Date(now);
      nextDay.setDate(nextDay.getDate() + 1);
      nextDay.setHours(syncHour, syncMinute, 0, 0);
      return nextDay;

    default:
      return now;
  }
}

// Function to sync jobs for an account
async function syncJobsForAccount(account) {
  console.log(`🔄 Starting automated sync for account: ${account.name}`);

  try {
    // Fetch jobs from Workiz API
    const workizUrl = `https://api.workiz.com/api/v1/${account.workizApiToken}/job/all/?start_date=2025-01-01&offset=0&records=100&only_open=false`;
    console.log(`🌐 Fetching from Workiz: ${workizUrl}`);

    const response = await fetch(workizUrl);
    if (!response.ok) {
      throw new Error(`Workiz API error: ${response.status}`);
    }

    const data = await response.json();
    if (!data.flag || !Array.isArray(data.data)) {
      throw new Error("Invalid response from Workiz API");
    }

    // Add accountId to each job
    const jobs = data.data.map((job) => ({
      ...job,
      accountId: account._id || account.id,
    }));

    // Upsert jobs into MongoDB
    const bulkOps = jobs.map((job) => ({
      updateOne: {
        filter: { UUID: job.UUID },
        update: { $set: job },
        upsert: true,
      },
    }));

    if (bulkOps.length > 0) {
      const bulkResult = await db.collection("jobs").bulkWrite(bulkOps);
      console.log(
        `✅ Jobs sync completed: ${bulkResult.upsertedCount} new, ${bulkResult.modifiedCount} updated`
      );
    }

    // Update existing jobs and clean up old ones
    console.log(`🔄 Starting job update and cleanup process...`);

    // Get all existing jobs for this account
    const existingJobs = await db
      .collection("jobs")
      .find({ accountId: account._id || account.id })
      .toArray();

    console.log(`📋 Found ${existingJobs.length} existing jobs in database`);

    // Calculate 30-day cutoff date
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

    let updatedJobsCount = 0;
    let deletedJobsCount = 0;
    let failedUpdatesCount = 0;

    // Process jobs in batches of 29 with 60-second delays
    const BATCH_SIZE = 29;
    const DELAY_BETWEEN_BATCHES = 60000; // 60 seconds in milliseconds

    for (let i = 0; i < existingJobs.length; i += BATCH_SIZE) {
      const batch = existingJobs.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(existingJobs.length / BATCH_SIZE);

      console.log(
        `📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} jobs)`
      );

      // Process each job in the current batch
      for (const existingJob of batch) {
        try {
          const jobDate = new Date(existingJob.JobDateTime);

          // Check if job is older than 30 days
          if (jobDate < thirtyDaysAgo) {
            console.log(
              `🗑️ Deleting old job: ${existingJob.UUID} (${existingJob.JobDateTime})`
            );
            await db.collection("jobs").deleteOne({ UUID: existingJob.UUID });
            deletedJobsCount++;
            continue;
          }

          // Update job using Workiz API
          console.log(`🔄 Updating job: ${existingJob.UUID}`);
          const updateUrl = `https://api.workiz.com/api/v1/${account.workizApiToken}/job/get/${existingJob.UUID}/`;

          const updateResponse = await fetch(updateUrl);
          if (updateResponse.ok) {
            const updateData = await updateResponse.json();

            if (updateData.flag && updateData.data) {
              // Update the job with fresh data from Workiz
              const updatedJob = {
                ...updateData.data,
                accountId: account._id || account.id,
              };

              await db
                .collection("jobs")
                .updateOne({ UUID: existingJob.UUID }, { $set: updatedJob });

              updatedJobsCount++;
              console.log(`✅ Updated job: ${existingJob.UUID}`);
            } else {
              console.log(
                `⚠️ Job not found in Workiz API: ${existingJob.UUID}`
              );
              // Job might have been deleted in Workiz, so delete from our database
              await db.collection("jobs").deleteOne({ UUID: existingJob.UUID });
              deletedJobsCount++;
            }
          } else {
            console.log(
              `❌ Failed to update job ${existingJob.UUID}: ${updateResponse.status}`
            );
            failedUpdatesCount++;
          }

          // Add a small delay between individual job updates (1000ms)
          await new Promise((resolve) => setTimeout(resolve, 1000));
        } catch (error) {
          console.log(
            `❌ Error processing job ${existingJob.UUID}: ${error.message}`
          );
          failedUpdatesCount++;
        }
      }

      // Add delay between batches (except for the last batch)
      if (i + BATCH_SIZE < existingJobs.length) {
        console.log(`⏳ Waiting 60 seconds before next batch...`);
        await new Promise((resolve) =>
          setTimeout(resolve, DELAY_BETWEEN_BATCHES)
        );
      }
    }

    console.log(`📊 Job update and cleanup completed:`);
    console.log(`   - Updated: ${updatedJobsCount} jobs`);
    console.log(`   - Deleted (old): ${deletedJobsCount} jobs`);
    console.log(`   - Failed updates: ${failedUpdatesCount} jobs`);

    // Record sync history
    const syncHistoryRecord = {
      accountId: account._id || account.id,
      syncType: "jobs",
      status: "success",
      details: {
        jobsFromWorkiz: jobs.length,
        finalJobCount: await db
          .collection("jobs")
          .countDocuments({ accountId: account._id || account.id }),
        jobsUpdated: updatedJobsCount,
        jobsDeleted: deletedJobsCount,
        failedUpdates: failedUpdatesCount,
      },
    };

    await db.collection("syncHistory").insertOne(syncHistoryRecord);
    console.log(`📝 Jobs sync history recorded`);

    // Update account's lastSyncDate and nextSyncDate
    const nextSyncDate = calculateNextSyncDate(account);
    await db.collection("accounts").updateOne(
      { _id: account._id || new ObjectId(account.id) },
      {
        $set: {
          lastSyncDate: new Date(),
          nextSyncDate: nextSyncDate,
        },
      }
    );

    return {
      success: true,
      jobsSynced: jobs.length,
      jobsUpdated: updatedJobsCount,
      jobsDeleted: deletedJobsCount,
      failedUpdates: failedUpdatesCount,
    };
  } catch (error) {
    console.error(
      `❌ Jobs sync failed for account ${account.name}:`,
      error.message
    );

    // Record failed sync history
    const syncHistoryRecord = {
      accountId: account._id || account.id,
      syncType: "jobs",
      status: "error",
      errorMessage: error.message,
      details: {},
    };
    await db.collection("syncHistory").insertOne(syncHistoryRecord);

    return { success: false, error: error.message };
  }
}

// Function to sync to Google Sheets for an account
async function syncToSheetsForAccount(account) {
  console.log(
    `📊 Starting automated Google Sheets sync for account: ${account.name}`
  );

  try {
    if (!account.googleSheetsId) {
      throw new Error("Missing Google Sheet ID");
    }

    // Get all jobs for this account
    const allJobs = await db
      .collection("jobs")
      .find({ accountId: account._id || account.id })
      .toArray();

    // Filter jobs by sourceFilter
    let filteredJobs = allJobs;
    if (
      account.sourceFilter &&
      Array.isArray(account.sourceFilter) &&
      account.sourceFilter.length > 0
    ) {
      filteredJobs = allJobs.filter((job) =>
        account.sourceFilter.includes(job.JobSource)
      );
    }

    if (filteredJobs.length === 0) {
      console.log(`⚠️ No jobs to sync for account ${account.name}`);
      return { success: true, jobsSynced: 0 };
    }

    // Parse Google Sheets credentials
    const credentialsStr =
      process.env.VITE_GOOGLE_SHEETS_CREDENTIALS ||
      process.env.GOOGLE_SHEETS_CREDENTIALS;
    if (!credentialsStr) {
      throw new Error("Google Sheets credentials not found");
    }

    const credentials = JSON.parse(credentialsStr);
    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });

    const sheets = google.sheets({ version: "v4", auth });

    // Clear the sheet first (skip header row)
    await sheets.spreadsheets.values.clear({
      spreadsheetId: account.googleSheetsId,
      range: "Sheet1!A2:F",
    });

    // Prepare data for Google Sheets
    const values = filteredJobs.map((job) => {
      const formattedTime =
        formatInTimeZone(
          new Date(job.JobDateTime),
          "America/Los_Angeles",
          "yyyy-MM-dd'T'HH:mm:ss"
        ) + " America/Los_Angeles";

      return [
        job.Phone || "",
        formattedTime,
        "Google Ads Convert",
        "",
        account.defaultConversionValue || 0,
        "USD",
      ];
    });

    // Add to Google Sheet
    const response = await sheets.spreadsheets.values.append({
      spreadsheetId: account.googleSheetsId,
      range: "Sheet1!A2:F",
      valueInputOption: "USER_ENTERED",
      requestBody: { values },
    });

    // Record sync history
    const syncHistoryRecord = {
      accountId: account._id || account.id,
      syncType: "sheets",
      status: "success",
      details: {
        totalJobs: allJobs.length,
        filteredJobs: filteredJobs.length,
        updatedRows: response.data.updates?.updatedRows || 0,
        sourceFilter: account.sourceFilter,
      },
    };

    await db.collection("syncHistory").insertOne(syncHistoryRecord);
    console.log(`📝 Sheets sync history recorded`);

    return {
      success: true,
      rowsUpdated: response.data.updates?.updatedRows || 0,
    };
  } catch (error) {
    console.error(
      `❌ Sheets sync failed for account ${account.name}:`,
      error.message
    );

    // Record failed sync history
    const syncHistoryRecord = {
      accountId: account._id || account.id,
      syncType: "sheets",
      status: "error",
      errorMessage: error.message,
      details: {},
    };
    await db.collection("syncHistory").insertOne(syncHistoryRecord);

    return { success: false, error: error.message };
  }
}

// Main cron function
export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  console.log("🕐 Cron job triggered at:", new Date().toISOString());

  try {
    await connectToMongoDB();

    // Get all accounts with sync enabled
    const accounts = await db
      .collection("accounts")
      .find({ syncEnabled: true })
      .toArray();
    console.log(`📋 Found ${accounts.length} accounts with sync enabled`);

    const results = [];

    for (const account of accounts) {
      if (shouldSyncNow(account)) {
        console.log(`⏰ Time to sync account: ${account.name}`);

        // Sync jobs
        const jobsResult = await syncJobsForAccount(account);
        results.push({
          account: account.name,
          jobsSync: jobsResult,
        });

        // Sync to sheets (if Google Sheets ID is configured)
        if (account.googleSheetsId) {
          const sheetsResult = await syncToSheetsForAccount(account);
          results.push({
            account: account.name,
            sheetsSync: sheetsResult,
          });
        }
      } else {
        console.log(
          `⏳ Not time to sync account: ${account.name} (next sync: ${account.nextSyncDate})`
        );
      }
    }

    console.log("✅ Cron job completed successfully");
    res.status(200).json({
      message: "Cron job completed",
      timestamp: new Date().toISOString(),
      results,
    });
  } catch (error) {
    console.error("❌ Cron job failed:", error);
    res.status(500).json({
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
}
