"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[6379],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>d});var n=r(67294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function l(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?l(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):l(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function o(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},l=Object.keys(e);for(n=0;n<l.length;n++)r=l[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(n=0;n<l.length;n++)r=l[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var s=n.createContext({}),p=function(e){var t=n.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},u=function(e){var t=p(e.components);return n.createElement(s.Provider,{value:t},e.children)},c="mdxType",v={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,l=e.originalType,s=e.parentName,u=o(e,["components","mdxType","originalType","parentName"]),c=p(r),m=a,d=c["".concat(s,".").concat(m)]||c[m]||v[m]||l;return r?n.createElement(d,i(i({ref:t},u),{},{components:r})):n.createElement(d,i({ref:t},u))}));function d(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var l=r.length,i=new Array(l);i[0]=m;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o[c]="string"==typeof e?e:a,i[1]=o;for(var p=2;p<l;p++)i[p]=r[p];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},88529:(e,t,r)=>{r.d(t,{Z:()=>a});var n=r(67294);const a=e=>{let{label:t,href:r}=e;return n.createElement("a",{href:r},t)}},48107:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>u,contentTitle:()=>s,default:()=>m,frontMatter:()=>o,metadata:()=>p,toc:()=>c});var n=r(87462),a=(r(67294),r(3905)),l=r(44996),i=r(88529);const o={slug:"/trevas-jupyter-0.3.2",title:"Trevas Jupyter 0.3.2",authors:["nicolas"],tags:["Trevas Jupyter"]},s=void 0,p={permalink:"/Trevas/zh-CN/blog/trevas-jupyter-0.3.2",source:"@site/i18n/zh-CN/docusaurus-plugin-content-blog/2023-07-01-v1-trevas-jupyter-0.3.2.mdx",title:"Trevas Jupyter 0.3.2",description:"Trevas Jupyter 0.3.2 uses version 1.0.2 of Trevas.",date:"2023-07-01T00:00:00.000Z",formattedDate:"2023\u5e747\u67081\u65e5",tags:[{label:"Trevas Jupyter",permalink:"/Trevas/zh-CN/blog/tags/trevas-jupyter"}],readingTime:.59,hasTruncateMarker:!1,authors:[{name:"Nicolas Laval",link:"https://github.com/NicoLaval",title:"Making Sense - Developer",image:"profile_pic_nicolas_laval.jpg",key:"nicolas"}],frontMatter:{slug:"/trevas-jupyter-0.3.2",title:"Trevas Jupyter 0.3.2",authors:["nicolas"],tags:["Trevas Jupyter"]},prevItem:{title:"Trevas Batch 0.1.1",permalink:"/Trevas/zh-CN/blog/trevas-batch-0.1.1"},nextItem:{title:"Trevas Lab 0.3.3",permalink:"/Trevas/zh-CN/blog/trevas-lab-0.3.3"}},u={authorsImageUrls:[void 0]},c=[{value:"News",id:"news",level:3},{value:"Launch",id:"launch",level:3},{value:"Manually adding the Trevas Kernel to an existing Jupyter instance",id:"manually-adding-the-trevas-kernel-to-an-existing-jupyter-instance",level:4},{value:"Docker",id:"docker",level:4},{value:"Helm",id:"helm",level:4}],v={toc:c};function m(e){let{components:t,...r}=e;return(0,a.kt)("wrapper",(0,n.Z)({},v,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("p",null,(0,a.kt)("a",{parentName:"p",href:"https://github.com/InseeFrLab/Trevas-Jupyter"},"Trevas Jupyter")," ",(0,a.kt)("inlineCode",{parentName:"p"},"0.3.2")," uses version ",(0,a.kt)("inlineCode",{parentName:"p"},"1.0.2")," of ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/InseeFr/Trevas"},"Trevas"),"."),(0,a.kt)("h3",{id:"news"},"News"),(0,a.kt)("p",null,"In addition to the ",(0,a.kt)(i.Z,{label:"VTL coverage",href:(0,l.Z)("/user-guide/coverage"),mdxType:"Link"})," greatly increased since the publication of Trevas 1.x.x, Trevas Jupyter offers 1 new connector:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"SAS files (via the ",(0,a.kt)("inlineCode",{parentName:"li"},"loadSas")," method)")),(0,a.kt)("h3",{id:"launch"},"Launch"),(0,a.kt)("h4",{id:"manually-adding-the-trevas-kernel-to-an-existing-jupyter-instance"},"Manually adding the Trevas Kernel to an existing Jupyter instance"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Trevas Jupyter compiler"),(0,a.kt)("li",{parentName:"ul"},"Copy the ",(0,a.kt)("inlineCode",{parentName:"li"},"kernel.json")," file and the ",(0,a.kt)("inlineCode",{parentName:"li"},"bin")," and ",(0,a.kt)("inlineCode",{parentName:"li"},"repo")," folders to a new kernel folder."),(0,a.kt)("li",{parentName:"ul"},"Edit the ",(0,a.kt)("inlineCode",{parentName:"li"},"kernel.json")," file"),(0,a.kt)("li",{parentName:"ul"},"Launch Jupyter")),(0,a.kt)("h4",{id:"docker"},"Docker"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"docker pull inseefrlab/trevas-jupyter:0.3.2\ndocker run -p 8888:8888 inseefrlab/trevas-jupyter:0.3.2\n")),(0,a.kt)("h4",{id:"helm"},"Helm"),(0,a.kt)("p",null,"The Trevas Jupyter docker image can be instantiated via the ",(0,a.kt)("inlineCode",{parentName:"p"},"jupyter-pyspark")," Helm contract from ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/InseeFrLab/helm-charts-interactive-services/tree/main"},"InseeFrLab"),"."))}m.isMDXComponent=!0}}]);