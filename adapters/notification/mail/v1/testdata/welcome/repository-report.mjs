/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';

const renderOnly = process.argv[2] === '--render';
const [outputArgument, libraryArgument, calendarArgument] = process.argv.slice(renderOnly ? 3 : 2);
if (!outputArgument || !libraryArgument || !calendarArgument) throw new Error('Usage: node repository-report.mjs [--render] <output-directory> <verified-echarts.min.js> <calendar-bundle.js>');
const output = path.resolve(outputArgument);
fs.mkdirSync(output, { recursive: true });
if (!renderOnly && fs.existsSync(path.join(output, 'snapshot.json'))) throw new Error('Refusing to overwrite a captured snapshot');
const calendarLibrary = fs.readFileSync(calendarArgument, 'utf8');
const library = fs.readFileSync(libraryArgument);
const blobHash = crypto.createHash('sha1').update(Buffer.from('blob ' + library.length + '\0')).update(library).digest('hex');
if (blobHash !== '3b8ed4bcd17f7c838d86d4920af588f1a0aeb389') throw new Error('Expected pinned ECharts 6.1.0 distribution');

const repository = 'frost-leo/fathomry';
const budgetEnd = Date.now() + 120000;
function get(endpoint) {
  for (let attempt = 0; attempt < 2; attempt++) {
    const remaining = budgetEnd - Date.now();
    if (remaining <= 0) throw new Error('GitHub capture budget exhausted');
    try {
      const browserSession = process.env.FATHOMRY_REPORT_BROWSER_SESSION;
      const browserCLI = process.env.FATHOMRY_REPORT_BROWSER_CLI;
      if (browserSession || browserCLI) {
        if (!browserSession || !browserCLI || !/^[A-Za-z0-9_-]+$/.test(browserSession)) throw new Error('Explicit browser reader configuration is incomplete');
        const code = 'async page => { const reply=await page.request.get(' + JSON.stringify('https://api.github.com/' + endpoint) +
          ',{timeout:15000,maxRedirects:0,headers:{Accept:"application/vnd.github+json","X-GitHub-Api-Version":"2026-03-10"}});' +
          'if(reply.status()!==200)throw new Error("GitHub HTTP "+reply.status()); const body=await reply.body();' +
          'if(body.length>2097152)throw new Error("GitHub response too large");return JSON.parse(body.toString()); }';
        const output = execFileSync('bash', [browserCLI, '-s=' + browserSession, 'run-code', code],
          { timeout: Math.min(20000, remaining), maxBuffer: 2 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'],
            env: { ...process.env, npm_config_offline: 'true' } }).toString();
        const match = output.match(/### Result\r?\n([\s\S]*?)\r?\n### /);
        if (!match) throw new Error('Browser reader did not return structured data');
        return JSON.parse(match[1]);
      }
      const data = execFileSync('curl', ['--http1.1', '--proto', '=https', '--tlsv1.2', '--fail', '--silent', '--show-error',
        '--connect-timeout', '5', '--max-time', '20', '-H', 'Accept: application/vnd.github+json',
        '-H', 'X-GitHub-Api-Version: 2026-03-10', 'https://api.github.com/' + endpoint],
        { timeout: Math.min(20000, remaining), maxBuffer: 2 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'] });
      return JSON.parse(data);
    } catch (error) {
      if (attempt === 1) throw new Error('Bounded GitHub read failed (' + (error.status ?? error.code ?? 'invalid-json') + '): ' + endpoint.split('?')[0]);
    }
  }
}
function integer(value) { return Number.isSafeInteger(value) && value >= 0; }
function date(value) {
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) throw new Error('Invalid GitHub date');
  return parsed;
}
function pages(endpoint, pageSize = 100, maxPages = 10) {
  const collected = [];
  for (let page = 1; page <= maxPages; page++) {
    const separator = endpoint.includes('?') ? '&' : '?';
    const items = get(endpoint + separator + 'per_page=' + pageSize + '&page=' + page);
    if (!Array.isArray(items) || items.length > pageSize) throw new Error('Invalid GitHub page');
    collected.push(...items);
    if (items.length < pageSize) return collected;
  }
  throw new Error('GitHub pagination exceeded the declared bound; no partial chart');
}

function capture() {
const meta = get('repos/' + repository);
if (meta.id !== 1360531276 || meta.private !== false || !integer(meta.stargazers_count) || !integer(meta.forks_count) ||
    typeof meta.default_branch !== 'string' || !/^[A-Za-z0-9._/-]{1,128}$/.test(meta.default_branch)) throw new Error('Unexpected repository identity');
const head = get('repos/' + repository + '/git/ref/heads/' + encodeURIComponent(meta.default_branch)).object?.sha;
if (typeof head !== 'string' || !/^[a-f0-9]{40}$/.test(head)) throw new Error('Invalid branch revision');
const captured = new Date();
const until = captured.toISOString();
const dayEnd = until.slice(0, 10);
const beginning = new Date(dayEnd + 'T00:00:00Z');
beginning.setUTCFullYear(beginning.getUTCFullYear() - 1);
beginning.setUTCDate(beginning.getUTCDate() + 1);
const since = beginning.toISOString();
const created = date(meta.created_at).toISOString().slice(0, 10);
const rawCommits = pages('repos/' + repository + '/commits?sha=' + head + '&since=' + encodeURIComponent(since) + '&until=' + encodeURIComponent(until))
  .map(commit => ({sha: commit.sha, date: commit.commit?.committer?.date}));
const daily = new Map();
for (let cursor = new Date(beginning); cursor <= captured; cursor.setUTCDate(cursor.getUTCDate() + 1)) daily.set(cursor.toISOString().slice(0, 10), 0);
const shas = new Set();
for (const commit of rawCommits) {
  if (!/^[a-f0-9]{40}$/.test(commit.sha) || shas.has(commit.sha)) throw new Error('Invalid or repeated commit');
  shas.add(commit.sha);
  const timestamp = date(commit.date);
  if (timestamp < beginning || timestamp > captured) continue;
  const day = timestamp.toISOString().slice(0, 10);
  if (!daily.has(day)) throw new Error('Commit outside calendar');
  daily.set(day, daily.get(day) + 1);
}
const months = [];
for (let offset = 11; offset >= 0; offset--) {
  const month = new Date(Date.UTC(captured.getUTCFullYear(), captured.getUTCMonth() - offset, 1)).toISOString().slice(0, 7);
  months.push({ month, issues: 0, prs: 0, applicable: month >= created.slice(0, 7) });
}
const monthMap = new Map(months.map(item => [item.month, item]));
// The issues API also returns PRs. Classify each record once, never double-count.
const rawIssues = pages('repos/' + repository + '/issues?state=all&sort=created&direction=desc&since=' + encodeURIComponent(since))
  .map(item => ({number: item.number, created_at: item.created_at, is_pr: Object.hasOwn(item, 'pull_request')}));
const numbers = new Set();
for (const item of rawIssues) {
  if (!Number.isSafeInteger(item.number) || item.number <= 0 || numbers.has(item.number) || typeof item.is_pr !== 'boolean') throw new Error('Invalid or repeated issue');
  numbers.add(item.number);
  const timestamp = date(item.created_at);
  if (timestamp > captured) continue;
  const month = monthMap.get(timestamp.toISOString().slice(0, 7));
  if (month) month[item.is_pr ? 'prs' : 'issues']++;
}
const languages = get('repos/' + repository + '/languages');
if (!languages || Array.isArray(languages) || typeof languages !== 'object' || Object.keys(languages).length > 64) throw new Error('Invalid languages result');
for (const [name, count] of Object.entries(languages)) {
  if (!name || name.length > 80 || !integer(count)) throw new Error('Invalid language bytes');
}
const weeks = pages('repos/' + repository + '/stargazers/history', 30, 4)
  .sort((a, b) => a.week - b.week);
for (let index = 0; index < weeks.length; index++) {
  const week = weeks[index];
  if (!integer(week.week) || !integer(week.total) || !Array.isArray(week.days) || week.days.length !== 7 ||
      !week.days.every(integer) || week.days.reduce((sum, value) => sum + value, 0) !== week.total ||
      index > 0 && week.week <= weeks[index - 1].week) throw new Error('Invalid star history');
}
const snapshot = {
  version: 1, repository, repository_id: meta.id, branch: meta.default_branch, head,
  captured_at: until, capture_completed_at: new Date().toISOString(), created_at: meta.created_at,
  since, stars: meta.stargazers_count, forks: meta.forks_count,
  commits: [...daily.values()].reduce((sum, value) => sum + value, 0),
  calendar: [...daily].map(([day, count]) => ({ day, count, applicable: day >= created || count > 0 })),
  months, languages,
  star_weeks: weeks.filter(week => week.week * 1000 + 7 * 86400000 >= beginning.getTime()),
  star_history_basis: 'New stars per GitHub service-defined week; not historical net star totals.'
};
fs.writeFileSync(path.join(output, 'snapshot.json'), JSON.stringify(snapshot, null, 2) + '\n');
return snapshot;
}
const snapshot = renderOnly ? JSON.parse(fs.readFileSync(path.join(output, 'snapshot.json'), 'utf8')) : capture();
const render = `async page => {
  await page.goto('about:blank');
  await page.setContent('<html lang="en"><body style="margin:0;background:#fff"><div id="chart" style="width:720px;height:330px"></div></body></html>');
  await page.addScriptTag({content:${JSON.stringify(library.toString())}});
  await page.addScriptTag({content:${JSON.stringify(calendarLibrary)}});
  if(await page.evaluate(()=>echarts.version)!=='6.1.0')throw new Error('Unexpected chart engine');
  const snapshot=${JSON.stringify(snapshot)};
  for(const kind of ['commits','stars','languages','issues']) {
    await page.evaluate(({snapshot,kind})=>{
      const node=document.getElementById('chart');
      const previous=echarts.getInstanceByDom(node); if(previous) previous.dispose();
      if(window.disposeFathomryCalendar){window.disposeFathomryCalendar();window.disposeFathomryCalendar=null;}
      node.style.padding=kind==='commits'?'18px 22px':'0';
      node.style.boxSizing='border-box';
      node.style.height=kind==='commits'?'auto':'330px';
      if(kind==='commits') {
        window.disposeFathomryCalendar=window.renderFathomryCalendar(node,snapshot);
        return;
      }
      const chart=echarts.init(node,null,{renderer:'canvas',devicePixelRatio:2});
      const purple='#AF52DE',blue='#007AFF',pink='#FF2D55';
      const option={animation:false,backgroundColor:'#fff',textStyle:{fontFamily:'Arial',color:'#707078'},tooltip:{show:false}};
      const axes=(labels)=>({
        grid:{left:60,right:30,top:30,bottom:58},
        xAxis:{type:'category',data:labels,axisTick:{show:false},axisLine:{lineStyle:{color:'#E5E5EA'}},axisLabel:{fontSize:13,interval:0}},
        yAxis:{type:'value',min:0,minInterval:1,axisLabel:{fontSize:13},splitLine:{lineStyle:{color:'#F0F0F4'}}}
      });
      if(kind==='stars') {
        const labels=snapshot.star_weeks.map(item=>new Date(item.week*1000).toISOString().slice(5,10));
        Object.assign(option,axes(labels));
        option.xAxis.boundaryGap=false;
        option.xAxis.axisLabel.interval=Math.max(0,Math.ceil(labels.length/8)-1);
        option.yAxis.max=Math.max(1,...snapshot.star_weeks.map(item=>item.total));
        option.series=[{type:'line',data:snapshot.star_weeks.map(item=>item.total),smooth:false,symbol:'circle',symbolSize:9,
          lineStyle:{color:purple,width:3},itemStyle:{color:purple,borderColor:'#fff',borderWidth:2},
          areaStyle:{color:new echarts.graphic.LinearGradient(0,0,0,1,[{offset:0,color:'rgba(175,82,222,0.20)'},{offset:1,color:'rgba(175,82,222,0.02)'}])},
          label:{show:snapshot.star_weeks.length<=12,position:'top',color:purple,fontSize:15}}];
        if(labels.length===0)option.graphic=[{type:'text',left:'center',top:'middle',style:{text:'No star-history buckets returned',fontSize:16,fill:'#707078'}}];
      } else if(kind==='languages') {
        const data=Object.entries(snapshot.languages).sort((a,b)=>b[1]-a[1]).map(([name,value])=>({name,value}));
        const total=data.reduce((sum,item)=>sum+item.value,0);
        Object.assign(option,{color:[blue,'#FF9500','#30B0C7',purple,'#34C759',pink],
          legend:{orient:'vertical',right:30,top:'middle',itemWidth:12,itemHeight:12,itemGap:22,
            formatter:name=>{const entry=data.find(item=>item.name===name);return name+'  '+(100*entry.value/total).toFixed(2)+'%';},
            textStyle:{fontSize:16,color:'#44444B'}},
          series:[{type:'pie',radius:['47%','73%'],center:['32%','50%'],label:{show:false},data}],
          graphic:[{type:'text',x:230,y:142,style:{text:data[0]?.name||'',fontSize:23,fontWeight:600,fill:'#3A3A3C',textAlign:'center'}},
            {type:'text',x:230,y:174,style:{text:total?(100*data[0].value/total).toFixed(2)+'%':'',fontSize:19,fill:'#707078',textAlign:'center'}}]});
        if(total===0){option.series=[];option.graphic=[{type:'text',left:'center',top:'middle',style:{text:'No language bytes reported',fontSize:16,fill:'#707078'}}];}
      } else {
        Object.assign(option,axes(snapshot.months.map(item=>item.month.slice(2))));
        option.grid.top=44;
        option.xAxis.axisLabel.fontSize=12;
        option.legend={top:0,left:60,data:['Issues opened','PRs opened'],textStyle:{fontSize:13}};
        option.series=[{name:'Issues opened',color:blue,key:'issues'},{name:'PRs opened',color:pink,key:'prs'}].map(series=>({
          name:series.name,type:'bar',barMaxWidth:17,itemStyle:{color:series.color,borderRadius:[3,3,0,0]},
          data:snapshot.months.map(item=>item.applicable?item[series.key]:null),
          label:{show:true,position:'top',fontSize:12,color:series.color,formatter:params=>params.value>0?params.value:''}
        }));
        const firstApplicable=snapshot.months.findIndex(item=>item.applicable);
        if(firstApplicable>0)option.series[0].markArea={silent:true,itemStyle:{color:'#FAFAFC'},label:{show:true,color:'#8E8E93',fontSize:12,position:'insideTop'},
          data:[[{name:'Before repository creation',xAxis:snapshot.months[0].month.slice(2)},{xAxis:snapshot.months[firstApplicable-1].month.slice(2)}]]};
      }
      chart.setOption(option);
    },{snapshot,kind});
    await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
    await page.locator('#chart').screenshot({path:${JSON.stringify(output)}+'/'+kind+'.png'});
  }
}`;
fs.writeFileSync(path.join(output, 'render-report.js'), render);
console.log(JSON.stringify({captured_at:snapshot.captured_at,branch:snapshot.branch,head:snapshot.head,stars:snapshot.stars,forks:snapshot.forks,commits:snapshot.commits,months:snapshot.months.filter(item=>item.applicable)}));
