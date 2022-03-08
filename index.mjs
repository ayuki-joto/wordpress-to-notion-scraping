import fetch from 'node-fetch';
import jsdom from 'jsdom';
import {parse} from 'csv-parse/sync';
import fs from 'fs';
import client from "@notionhq/client"
import dotenv from 'dotenv';
import {} from 'dotenv/config'

const {JSDOM} = jsdom;
const {Client} = client;

(async () => {
    const notion = new Client({auth: process.env.NOTION_TOKEN});
    const files = await read_csv(process.argv[2])
    const articles = await files.map(file => get_article(file['URLs'], file['Categories']))
})();


async function read_csv(file_path) {
    let data = fs.readFileSync(file_path);
    return parse(data, {
        columns: true,
        skip_empty_lines: true
    })
}

async function get_article(url, category) {
    const res = await fetch(url);
    const html = await res.text();
    const dom = new JSDOM(html);
    const document = dom.window.document;
    const nodes = document.getElementsByTagName('article');
    const children = nodes[0].children;

    const items = Array.from(children, element => element.innerHTML);

    if (items[0] === 'NOT FOUND') {
        return null
    }
    return {
        "title": document.getElementsByTagName('title')[0].text,
        "body": items.reduce((prev, current) => {
            return prev + current;
        }),
        "categories": category ? category.split(',') : null
    };
}

