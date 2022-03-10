import fetch from 'node-fetch';
import jsdom from 'jsdom';
import {parse} from 'csv-parse/sync';
import fs from 'fs';
import client from "@notionhq/client"
import dotenv from 'dotenv';
import {} from 'dotenv/config';
import {NodeHtmlMarkdown} from 'node-html-markdown';
import {markdownToBlocks} from '@tryfabric/martian';
import * as AWS from "@aws-sdk/client-s3";
import {v4 as uuid} from 'uuid';
import path from 'path';


const {JSDOM} = jsdom;
const {Client} = client;
const s3 = new AWS.S3({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY,
        secretAccessKey: process.env.AWS_SECRET,
    },
});

(async () => {
    const notion = new Client({auth: process.env.NOTION_TOKEN});
    const nhm = new NodeHtmlMarkdown({keepDataImages: true});
    const files = await read_csv(process.argv[2]);
    const articles = await Promise.all(files.map(async file => await get_article(nhm, file['URLs'], file['Categories'])));
    articles.map(async article => !!article ? await create_notion_page(notion, article) : null);
})();


async function read_csv(file_path) {
    let data = fs.readFileSync(file_path);
    return parse(data, {
        columns: true,
        skip_empty_lines: true
    })
}

async function get_article(nhm, url, category) {
    const res = await fetch(url);
    const html = await res.text();
    const dom = new JSDOM(html);
    const document = dom.window.document;
    const nodes = document.getElementsByTagName('article');
    const children = nodes[0].children;
    const items = Array.from(children, element => element.outerHTML);

    if (items[0].indexOf("NOT FOUND") !== -1) {
        return null
    }

    let body = items.reduce((prev, current) => {
        return prev + current;
    });
    let cover = await parseOgpImage([...document.head.querySelectorAll('meta')]);
    return {
        "url": url,
        "title": document.getElementsByTagName('title')[0].text,
        "body": nhm.translate(body),
        "categories": category ? category.split(',') : null,
        "cover": cover
    };
}

async function create_notion_page(notion, article) {
    let tags = [];
    try {
        let body = markdownToBlocks(article.body);
        body = await Promise.all(body.flatMap(async object => object.type === 'image' ? await uploadS3(object) : object));
        if (article.categories) {
            article.categories.map(category => tags.push({"name": category}));
        }

        const response = await notion.pages.create({
            parent: {
                database_id: process.env.NOTION_DB_TOKEN,
            },
            cover: {
                type: "external",
                external: {
                    url: article.cover
                }
            },
            properties: {
                Name: {
                    title: [
                        {
                            text: {
                                content: article.title,
                            },
                        },
                    ],
                },
                Tags: {
                    multi_select: tags
                }
            },
            children: body
        });
    } catch (exception) {
        console.log(article.url);
        console.log(exception);
    }
}

async function uploadS3(block) {
    const src = block.image.external.url;
    if (!src) return block;

    const params = {
        Bucket: process.env.S3_BUCKET_NAME,
    }

    if (/^data:/i.test(src)) {
        const fileData = src.replace(/^data:\w+\/\w+;base64,/, '')
        const decodedFile = Buffer.from(fileData, 'base64')
        const fileExtension = src.toString().slice(src.indexOf('/') + 1, src.indexOf(';'))
        const contentType = src.toString().slice(src.indexOf(':') + 1, src.indexOf(';'))
        params['Body'] = decodedFile;
        params['Key'] = [process.env.S3_PREFIX, uuid(), fileExtension].join('.');
        params['ContentType'] = contentType;
        try {
            await s3.putObject(params).then(() => {
                block.image.external.url = [process.env.CLOUD_FRONT_URL, params['Key']].join('/');
            });
            return block;
        } catch (err) {
            console.error('ERROR:', err);
            return block;
        }
    } else {
        try {
            const response = await fetch(src)
            params['ContentType'] = response.headers.get("content-type") ?? undefined;
            params['ContentLength'] = response.headers.get("content-length") != null
                ? Number(response.headers.get("content-length"))
                : undefined;
            params['Body'] = response.body;
            params['Key'] = [process.env.S3_PREFIX, uuid(), path.extname(src)].join('');
            await s3.putObject(params)
                .then(() => {
                    block.image.external.url = [process.env.CLOUD_FRONT_URL, params['Key']].join('/');
                });
            return block;
        } catch (err) {
            console.error('ERROR:', err);
            return block;
        }
    }
}

async function parseOgpImage(allMetaTag) {
    const ogp = allMetaTag
        .filter((element) => {
            const property = element.getAttribute("property")?.trim();
            return !(!property || property !== 'og:image');
        });
    if (ogp[0]) {
        const src = ogp[0].getAttribute("content");
        const params = {
            Bucket: process.env.S3_BUCKET_NAME,
        }
        try {
            const response = await fetch(src)
            params['ContentType'] = response.headers.get("content-type") ?? undefined;
            params['ContentLength'] = response.headers.get("content-length") != null
                ? Number(response.headers.get("content-length"))
                : undefined;
            params['Body'] = response.body;
            params['Key'] = [process.env.S3_COVER_PREFIX, uuid(), path.extname(src)].join('');
            await s3.putObject(params)
                .then(() => {
                    return [process.env.CLOUD_FRONT_URL, params['Key']].join('/');
                }).catch(err => {
                    console.error('ERROR:', err);
                    console.log('hay');
                    return null;
                });
            return [process.env.CLOUD_FRONT_URL, params['Key']].join('/');
        } catch (err) {
            console.error('ERROR:', err);
            console.log('hay');
            return null;
        }
    } else {
        return null;
    }
}
