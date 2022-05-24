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
import {PutObjectCommand} from "@aws-sdk/client-s3";
import dayjs from 'dayjs';

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
    const nhm = new NodeHtmlMarkdown({
            keepDataImages: true,
        },
        {
            iframe: {
                preserveIfEmpty: true,
                postprocess: ({node, options}) => {
                    let src = node.getAttribute('src') || '';
                    if (!src || (!options.keepDataImages && /^data:/i.test(src)))
                        return {ignore: true};
                    if (!src.includes('https:')) {
                        src = 'https:' + src
                    }
                    const alt = node.getAttribute('alt') || '';
                    return `![${alt}](${src})`
                }
            }
        });
    const files = await read_csv(process.argv[2]);
    const type = process.argv[3];
    const articles = await Promise.all(files.map(async file => await get_article(nhm, file['URLs'], file['Categories'], type)));
    if (type === 'news') {
        articles.map(async article => !!article ? await create_news_notion_page(notion, article) : null);
    } else if (type === 'voice') {
        articles.map(async article => !!article ? await create_voice_notion_page(notion, article) : null);
    } else if (type === 'activity') {
        articles.map(async article => !!article ? await create_activity_notion_page(notion, article) : null);
    }
})();


async function read_csv(file_path) {
    let data = fs.readFileSync(file_path);
    return parse(data, {
        columns: true,
        skip_empty_lines: true
    })
}

async function get_article(nhm, uri, category, type) {
    const res = await fetch(uri);
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
    let slug = null;
    if (body.includes('プレスリリース')) {
        const mainNode = document.getElementsByTagName('main')
        const mainChildren = mainNode[0].children;
        const script = mainChildren[0].outerHTML;
        slug = script.split('"')[1];
        body = '';
    }
    let thumbnail = await parseOgpImage([...document.head.querySelectorAll('meta')]);
    let published_at = '';
    if (type !== 'activity') {
        published_at = dayjs(children[1].innerHTML.match(/\d{4}.\d{1,2}.\d{1,2}/)[0], 'YYYY.MM.DD').format();
    } else {
        published_at = dayjs().format();
    }

    return {
        "url": uri,
        "title": document.getElementsByTagName('title')[0].text,
        "body": nhm.translate(body),
        "tags": category ? category.split(', ') : null,
        "thumbnail": thumbnail,
        "published_at": published_at,
        "slug": slug === null ? decodeURI(getSlugUseType(uri, type)) : slug
    };
}

function getSlugUseType(uri, type) {
    if (type === 'voice') {
        return uri.split('/').pop().split('=').pop();
    }
    return uri.split('/').pop();
}

async function create_voice_notion_page(notion, article) {
    try {
        let body = markdownToBlocks(article.body);
        body = await Promise.all(body.flatMap(async object => object.type === 'image' ? await uploadS3(object) : object));
        let thumbnail = [];
        if (article.thumbnail) {
            thumbnail.push({
                type: "external",
                name: article.thumbnail,
                external: {
                    url: article.thumbnail
                }
            });
        }
        body = body.filter(b => b !== null)
        await notion.pages.create({
            parent: {
                database_id: process.env.NOTION_VOICE_DB_TOKEN,
            },
            properties: {
                Page: {
                    title: [
                        {
                            text: {
                                content: article.title,
                            },
                        },
                    ],
                },
                slug: {
                    rich_text: [
                        {
                            type: "text",
                            text: {
                                "content": article.slug
                            }
                        },
                    ]
                },
                published: {
                    checkbox: true,
                },
                published_at: {
                    date: {
                        "start": article.published_at,
                        "end": null
                    },
                },
                thumbnail: {
                    files: thumbnail
                },
            },
            children: body
        });
    } catch (exception) {
        console.log(article.url);
        console.log(exception);
    }
}

async function create_activity_notion_page(notion, article) {
    try {
        let body = markdownToBlocks(article.body);
        body = await Promise.all(body.flatMap(async object => object.type === 'image' ? await uploadS3(object) : object));
        body = body.filter(b => b !== null)
        let thumbnail = [];
        if (article.thumbnail) {
            thumbnail.push({
                type: "external",
                name: article.thumbnail,
                external: {
                    url: article.thumbnail
                }
            });
        }
        await notion.pages.create({
            parent: {
                database_id: process.env.NOTION_ACTIVITY_DB_TOKEN,
            },
            properties: {
                Page: {
                    title: [
                        {
                            text: {
                                content: article.title,
                            },
                        },
                    ],
                },
                slug: {
                    rich_text: [
                        {
                            type: "text",
                            text: {
                                "content": article.slug
                            }
                        },
                    ]
                },
                published: {
                    checkbox: true,
                },
                published_at: {
                    date: {
                        "start": article.published_at,
                        "end": null
                    },
                },
                thumbnail: {
                    files: thumbnail
                },
            },
            children: body
        });
    } catch (exception) {
        console.log(article.url);
        console.log(exception);
    }
}

async function create_news_notion_page(notion, article) {
    let tags = [];
    try {
        let body = markdownToBlocks(article.body);
        body = await Promise.all(body.flatMap(async object => object.type === 'image' ? await uploadS3(object) : object));
        body = body.filter(b => b !== null)
        if (article.tags) {
            article.tags = article.tags.filter(tag => tag !== 'ニュース');
            article.tags.map(tag => tags.push({"name": tag}));
        }
        let thumbnail = [];
        if (article.thumbnail) {
            thumbnail.push({
                type: "external",
                name: article.thumbnail,
                external: {
                    url: article.thumbnail
                }
            });
        }
        await notion.pages.create({
            parent: {
                database_id: process.env.NOTION_NEWS_DB_TOKEN,
            },
            properties: {
                Page: {
                    title: [
                        {
                            text: {
                                content: article.title,
                            },
                        },
                    ],
                },
                slug: {
                    rich_text: [
                        {
                            type: "text",
                            text: {
                                "content": article.slug
                            }
                        },
                    ]
                },
                Tags: {
                    multi_select: tags
                },
                published: {
                    checkbox: true,
                },
                published_at: {
                    date: {
                        "start": article.published_at,
                        "end": null
                    },
                },
                thumbnail: {
                    files: thumbnail
                },
            },
            children: body
        });
    } catch (exception) {
        console.log(article.url);
        console.log(exception);
    }
}

async function uploadS3(block) {
    let src = block.image.external.url;
    if (!src) return block;
    if (src.includes('embed') || src.includes('speakerdeck.com/player')) {
        return {
            "type": "embed",
            "embed": {
                "url": src
            }
        }
    }
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
            if (new URL(src).host === 'www.old.code4japan.org') {
                let tem_url  = new URL(src)
                tem_url.hostname = 'old.code4japan.org'
                src = tem_url.href
            }

            const response = await fetch(src)
            if (response.status === 404) {
                return null
            }
            params['ContentType'] = response.headers.get("content-type") ?? undefined;
            if (params['ContentType'] === 'text/plain') {
                return null
            }
            if (path.extname(src) === '' && params['ContentType'] !== undefined) {
                params['Key'] = [process.env.S3_PREFIX, uuid(), '.' ,params['ContentType'].split('/').pop()].join('');
            } else {
                params['Key'] = [process.env.S3_PREFIX, uuid(), path.extname(src)].join('');
            }
            if (response.headers.get("content-length") != null) {
                params['Body'] = response.body;
                params['ContentLength'] = Number(response.headers.get("content-length"));
                block = await s3.putObject(params)
                    .then(() => {
                        block.image.external.url = [process.env.CLOUD_FRONT_URL, params['Key']].join('/');
                    }).then(() => {
                        return block;
                    });
                return block
            } else {
                await streamToBuffer(response.body)
                    .then((buffer) => {
                        params['Body'] = buffer;
                        s3.send(new PutObjectCommand(params))
                    }).then(() => {
                        block.image.external.url = [process.env.CLOUD_FRONT_URL, params['Key']].join('/');
                    }).then(() => {
                        return block;
                    });
            }
        } catch (err) {
            console.error('ERROR:', err);
            return block
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
        let src = ogp[0].getAttribute("content");
        const params = {
            Bucket: process.env.S3_BUCKET_NAME,
        }
        try {
            if (new URL(src).host === 'www.old.code4japan.org') {
                 let tem_url  = new URL(src)
                tem_url.hostname = 'old.code4japan.org'
                src = tem_url.href
            }
            const response = await fetch(src)
            params['ContentType'] = response.headers.get("content-type") ?? undefined;
            params['ContentLength'] = response.headers.get("content-length") != null
                ? Number(response.headers.get("content-length"))
                : undefined;
            params['Body'] = response.body;
            if (path.extname(src).indexOf('?') !== -1) {
                params['Key'] = [process.env.S3_COVER_PREFIX, uuid(), path.extname(src).split('?')[0]].join('');
            } else {
                params['Key'] = [process.env.S3_COVER_PREFIX, uuid(), path.extname(src)].join('');
            }
            await s3.putObject(params)
                .then(() => {
                    return [process.env.CLOUD_FRONT_URL, params['Key']].join('/');
                }).catch(err => {
                    console.error('ERROR:', err);
                    return null;
                });
            return [process.env.CLOUD_FRONT_URL, params['Key']].join('/');
        } catch (err) {
            console.error('ERROR:', err);
            return null;
        }
    } else {
        return null;
    }
}

async function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
        const _buf = [];
        stream.on('data', (chunk) => _buf.push(chunk));
        stream.on('end', () => resolve(Buffer.concat(_buf)));
        stream.on('error', (err) => reject(err));
    });
}