"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[9966],{3905:(e,t,n)=>{n.d(t,{Zo:()=>c,kt:()=>d});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=a.createContext({}),p=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},c=function(e){var t=p(e.components);return a.createElement(s.Provider,{value:t},e.children)},u="mdxType",v={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,c=i(e,["components","mdxType","originalType","parentName"]),u=p(n),m=r,d=u["".concat(s,".").concat(m)]||u[m]||v[m]||o;return n?a.createElement(d,l(l({ref:t},c),{},{components:n})):a.createElement(d,l({ref:t},c))}));function d(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,l=new Array(o);l[0]=m;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i[u]="string"==typeof e?e:r,l[1]=i;for(var p=2;p<o;p++)l[p]=n[p];return a.createElement.apply(null,l)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},3447:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>u,frontMatter:()=>o,metadata:()=>i,toc:()=>p});var a=n(7462),r=(n(7294),n(3905));const o={slug:"/trevas-java-17",title:"Trevas - Java 17",authors:["nicolas"],tags:["Trevas"]},l=void 0,i={permalink:"/Trevas/no/blog/trevas-java-17",source:"@site/blog/2023-11-22-trevas-java-17.mdx",title:"Trevas - Java 17",description:"News",date:"2023-11-22T00:00:00.000Z",formattedDate:"22. november 2023",tags:[{label:"Trevas",permalink:"/Trevas/no/blog/tags/trevas"}],readingTime:.345,hasTruncateMarker:!1,authors:[{name:"Nicolas Laval",link:"https://github.com/NicoLaval",title:"Making Sense - Developer",image:"profile_pic_nicolas_laval.jpg",key:"nicolas"}],frontMatter:{slug:"/trevas-java-17",title:"Trevas - Java 17",authors:["nicolas"],tags:["Trevas"]},nextItem:{title:"Trevas - Persistent assignments",permalink:"/Trevas/no/blog/trevas-persistent-assignments"}},s={authorsImageUrls:[void 0]},p=[{value:"News",id:"news",level:3},{value:"Java modules handling",id:"java-modules-handling",level:3},{value:"Maven",id:"maven",level:4},{value:"Docker",id:"docker",level:4}],c={toc:p};function u(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h3",{id:"news"},"News"),(0,r.kt)("p",null,"Trevas 1.2.0 enables Java 17 support."),(0,r.kt)("h3",{id:"java-modules-handling"},"Java modules handling"),(0,r.kt)("p",null,"Spark does not support Java modules."),(0,r.kt)("p",null,"Java 17 client apps, embedding Trevas in Spark mode have to configure ",(0,r.kt)("inlineCode",{parentName:"p"},"UNNAMED")," modules for Spark."),(0,r.kt)("h4",{id:"maven"},"Maven"),(0,r.kt)("p",null,"Add to your ",(0,r.kt)("inlineCode",{parentName:"p"},"pom.xml")," file, in the ",(0,r.kt)("inlineCode",{parentName:"p"},"build > plugins")," section:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-xml"},"<plugin>\n    <groupId>org.apache.maven.plugins</groupId>\n    <artifactId>maven-compiler-plugin</artifactId>\n    <version>3.11.0</version>\n    <configuration>\n        <compilerArgs>\n            <arg>--add-exports</arg>\n            <arg>java.base/sun.nio.ch=ALL-UNNAMED</arg>\n        </compilerArgs>\n    </configuration>\n</plugin>\n")),(0,r.kt)("h4",{id:"docker"},"Docker"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},'ENTRYPOINT ["java", "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED", "mainClass"]\n')))}u.isMDXComponent=!0}}]);