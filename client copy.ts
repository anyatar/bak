import axios from "axios";
import * as fs from 'fs';
import { exit } from "process";
import * as readline from 'readline';

require("dotenv").config();

const EVENTS_FILE = process.env.EVENTS_FILE || 'events.jsonl';
const EVENT_SERVER_PORT = process.env.EVENT_SERVER_PORT || 8000;
const EVENT_SERVER_URL = process.env.EVENT_SERVER_URL || `http://localhost:${EVENT_SERVER_PORT}`;
const SECRET_VALUE = process.env.SECRET_VALUE || "secret";
const MAX_CONCURRENT_REQUESTS = 64;
const processLines: string[] = [];

class EventHandler {
    
    static async readJsonlFile() {
        const fileStream = fs.createReadStream(EVENTS_FILE).setEncoding('utf8');
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity,
        });
    
        for await (const line of rl) {
            await EventHandler.processLine(line);
        }
    }

    static async processLine(line: string) {
        try {
            const event = JSON.parse(line);
            if (EventHandler.validateEvent(event)) {
                //console.log(event);
                EventHandler.sendEvent(event);
            } else {
                console.log(`Skipping invalid event: ${line}`);
            }
        } catch (error) {
            console.error(`Error processing line: ${line}. Error: ${error}`);
        }
    }

    static validateEvent(event: any): boolean {
        if (
            typeof event.userId === 'string' &&
            (event.name === 'add_revenue' || event.name === 'subtract_revenue') &&
            Number.isInteger(event.value)
        ) {
            return true;
        } else {
            return false;
        }
    }

    static async sendEvent(event: any) {
       
        try {
            const response = await axios.post(`${EVENT_SERVER_URL}/liveEvent`,
                event,
                {
                    headers: {
                        'Authorization': SECRET_VALUE
                    }
                }
            );
            if (response.status !== 200) {
                console.error('Error in liveEvent request:', response.data);
            }

        } catch (error:any) {
            const typedError = error as Error;
            console.error('Error in liveEvent request:', typedError.message, typedError.name, activeConcurrentReq);
        }
    }
}

async function main() {
    console.log("Starting to generate events...");
    try {
        await EventHandler.readJsonlFile();
    } catch (error) {
        console.error("Error generating events", error);
    }
    console.log("Finished to generate events...");
}

main();