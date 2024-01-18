"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[6246],{3905:(e,t,r)=>{r.d(t,{Zo:()=>p,kt:()=>m});var a=r(67294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function l(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},o=Object.keys(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var i=a.createContext({}),u=function(e){var t=a.useContext(i),r=t;return e&&(r="function"==typeof e?e(t):l(l({},t),e)),r},p=function(e){var t=u(e.components);return a.createElement(i.Provider,{value:t},e.children)},c="mdxType",v={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},b=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,o=e.originalType,i=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),c=u(r),b=n,m=c["".concat(i,".").concat(b)]||c[b]||v[b]||o;return r?a.createElement(m,l(l({ref:t},p),{},{components:r})):a.createElement(m,l({ref:t},p))}));function m(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=r.length,l=new Array(o);l[0]=b;var s={};for(var i in t)hasOwnProperty.call(t,i)&&(s[i]=t[i]);s.originalType=e,s[c]="string"==typeof e?e:n,l[1]=s;for(var u=2;u<o;u++)l[u]=r[u];return a.createElement.apply(null,l)}return a.createElement.apply(null,r)}b.displayName="MDXCreateElement"},88529:(e,t,r)=>{r.d(t,{Z:()=>n});var a=r(67294);const n=e=>{let{label:t,href:r}=e;return a.createElement("a",{href:r},t)}},82433:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>p,contentTitle:()=>i,default:()=>b,frontMatter:()=>s,metadata:()=>u,toc:()=>c});var a=r(87462),n=(r(67294),r(3905)),o=r(44996),l=r(88529);const s={slug:"/trevas-lab-0.3.3",title:"Trevas Lab 0.3.3",authors:["nicolas"],tags:["Trevas Lab"]},i=void 0,u={permalink:"/Trevas/fr/blog/trevas-lab-0.3.3",source:"@site/i18n/fr/docusaurus-plugin-content-blog/2023-07-01-v1-trevas-lab-0.3.3.mdx",title:"Trevas Lab 0.3.3",description:"Trevas Lab 0.3.3 utilise la version 1.0.2 de Trevas.",date:"2023-07-01T00:00:00.000Z",formattedDate:"1 juillet 2023",tags:[{label:"Trevas Lab",permalink:"/Trevas/fr/blog/tags/trevas-lab"}],readingTime:.35,hasTruncateMarker:!1,authors:[{name:"Nicolas Laval",link:"https://github.com/NicoLaval",title:"Making Sense - D\xe9veloppeur",image:"profile_pic_nicolas_laval.jpg",key:"nicolas"}],frontMatter:{slug:"/trevas-lab-0.3.3",title:"Trevas Lab 0.3.3",authors:["nicolas"],tags:["Trevas Lab"]},prevItem:{title:"Trevas Jupyter 0.3.2",permalink:"/Trevas/fr/blog/trevas-jupyter-0.3.2"}},p={authorsImageUrls:[void 0]},c=[{value:"Nouveaut\xe9s",id:"nouveaut\xe9s",level:3},{value:"Lancement",id:"lancement",level:3},{value:"Kubernetes",id:"kubernetes",level:4}],v={toc:c};function b(e){let{components:t,...r}=e;return(0,n.kt)("wrapper",(0,a.Z)({},v,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("p",null,(0,n.kt)("a",{parentName:"p",href:"https://github.com/InseeFrLab/Trevas-Lab"},"Trevas Lab")," ",(0,n.kt)("inlineCode",{parentName:"p"},"0.3.3")," utilise la version ",(0,n.kt)("inlineCode",{parentName:"p"},"1.0.2")," de ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/InseeFr/Trevas"},"Trevas"),"."),(0,n.kt)("h3",{id:"nouveaut\xe9s"},"Nouveaut\xe9s"),(0,n.kt)("p",null,"En suppl\xe9ment de la ",(0,n.kt)(l.Z,{label:"couverture VTL",href:(0,o.Z)("/user-guide/coverage"),mdxType:"Link"})," largement augment\xe9e depuis la publication de Trevas 1.x.x, Trevas Lab propose 2 nouveaux connecteurs :"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"fichiers SAS"),(0,n.kt)("li",{parentName:"ul"},"JDBC MariaDB")),(0,n.kt)("h3",{id:"lancement"},"Lancement"),(0,n.kt)("h4",{id:"kubernetes"},"Kubernetes"),(0,n.kt)("p",null,"Des exemples d'objet Kubernetes sont disponibles dans les dossiers ",(0,n.kt)("inlineCode",{parentName:"p"},".kubernetes")," de ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/InseeFrLab/Trevas-Lab/tree/master/.kubernetes"},"Trevas Lab")," et ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/InseeFrLab/Trevas-Lab-UI/tree/master/.kubernetes"},"Trevas Lab UI"),"."))}b.isMDXComponent=!0}}]);