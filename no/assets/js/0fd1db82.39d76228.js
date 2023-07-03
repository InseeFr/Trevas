"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[1391],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>v});var a=r(7294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function l(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function i(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},o=Object.keys(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var c=a.createContext({}),s=function(e){var t=a.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):l(l({},t),e)),r},u=function(e){var t=s(e.components);return a.createElement(c.Provider,{value:t},e.children)},p="mdxType",h={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},f=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,o=e.originalType,c=e.parentName,u=i(e,["components","mdxType","originalType","parentName"]),p=s(r),f=n,v=p["".concat(c,".").concat(f)]||p[f]||h[f]||o;return r?a.createElement(v,l(l({ref:t},u),{},{components:r})):a.createElement(v,l({ref:t},u))}));function v(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=r.length,l=new Array(o);l[0]=f;var i={};for(var c in t)hasOwnProperty.call(t,c)&&(i[c]=t[c]);i.originalType=e,i[p]="string"==typeof e?e:n,l[1]=i;for(var s=2;s<o;s++)l[s]=r[s];return a.createElement.apply(null,l)}return a.createElement.apply(null,r)}f.displayName="MDXCreateElement"},8529:(e,t,r)=>{r.d(t,{Z:()=>n});var a=r(7294);const n=e=>{let{label:t,href:r}=e;return a.createElement("a",{href:r},t)}},8758:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>c,contentTitle:()=>l,default:()=>p,frontMatter:()=>o,metadata:()=>i,toc:()=>s});var a=r(7462),n=(r(7294),r(3905));r(4996),r(8529);const o={slug:"/trevas-batch-0.1.1",title:"Trevas Batch 0.1.1",authors:["nicolas"],tags:["Trevas Batch"]},l=void 0,i={permalink:"/Trevas/no/blog/trevas-batch-0.1.1",source:"@site/i18n/no/docusaurus-plugin-content-blog/2023-07-02-trevas-batch-0.1.1.mdx",title:"Trevas Batch 0.1.1",description:"Trevas Batch 0.1.1 uses version 1.0.2 of Trevas.",date:"2023-07-02T00:00:00.000Z",formattedDate:"2. juli 2023",tags:[{label:"Trevas Batch",permalink:"/Trevas/no/blog/tags/trevas-batch"}],readingTime:.46,hasTruncateMarker:!1,authors:[{name:"Nicolas Laval",link:"https://github.com/NicoLaval",title:"Making Sense - Developer",image:"profile_pic_nicolas_laval.jpg",key:"nicolas"}],frontMatter:{slug:"/trevas-batch-0.1.1",title:"Trevas Batch 0.1.1",authors:["nicolas"],tags:["Trevas Batch"]},nextItem:{title:"Trevas Jupyter 0.3.2",permalink:"/Trevas/no/blog/trevas-jupyter-0.3.2"}},c={authorsImageUrls:[void 0]},s=[{value:"Launch",id:"launch",level:3},{value:"Local",id:"local",level:4},{value:"Kubernetes",id:"kubernetes",level:4}],u={toc:s};function p(e){let{components:t,...r}=e;return(0,n.kt)("wrapper",(0,a.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("p",null,(0,n.kt)("a",{parentName:"p",href:"https://github.com/Making-Sense-Info/Trevas-Batch"},"Trevas Batch")," ",(0,n.kt)("inlineCode",{parentName:"p"},"0.1.1")," uses version ",(0,n.kt)("inlineCode",{parentName:"p"},"1.0.2")," of ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/InseeFr/Trevas"},"Trevas"),"."),(0,n.kt)("p",null,"This Java batch provides Trevas execution metrics in Spark mode."),(0,n.kt)("p",null,"The configuration file to fill in is described in the ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/Making-Sense-Info/Trevas-Batch/tree/main#readme"},"README")," of the project.\nLaunching the batch will produce a Markdown file as output."),(0,n.kt)("h3",{id:"launch"},"Launch"),(0,n.kt)("h4",{id:"local"},"Local"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-java"},'java -jar trevas-batch-0.1.1.jar -Dconfig.path="..." -Dreport.path="..."\n')),(0,n.kt)("p",null,"The java execution will be done in local Spark."),(0,n.kt)("h4",{id:"kubernetes"},"Kubernetes"),(0,n.kt)("p",null,"Default Kubernetes objects are defined in the ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/Making-Sense-Info/Trevas-Batch/tree/main/.kubernetes"},".kubernetes")," folder."),(0,n.kt)("p",null,"Feed the ",(0,n.kt)("inlineCode",{parentName:"p"},"config-map.yml")," file then launch the job in your cluster."))}p.isMDXComponent=!0}}]);