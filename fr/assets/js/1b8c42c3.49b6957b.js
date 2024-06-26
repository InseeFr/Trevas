"use strict";(self.webpackChunktrevas_documentation=self.webpackChunktrevas_documentation||[]).push([[409],{3905:(e,t,n)=>{n.d(t,{Zo:()=>u,kt:()=>k});var a=n(67294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=a.createContext({}),p=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},u=function(e){var t=p(e.components);return a.createElement(s.Provider,{value:t},e.children)},d="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},c=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),d=p(n),c=r,k=d["".concat(s,".").concat(c)]||d[c]||m[c]||o;return n?a.createElement(k,i(i({ref:t},u),{},{components:n})):a.createElement(k,i({ref:t},u))}));function k(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,i=new Array(o);i[0]=c;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[d]="string"==typeof e?e:r,i[1]=l;for(var p=2;p<o;p++)i[p]=n[p];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}c.displayName="MDXCreateElement"},18707:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>i,default:()=>d,frontMatter:()=>o,metadata:()=>l,toc:()=>p});var a=n(87462),r=(n(67294),n(3905));const o={slug:"/trevas-temporal-operators",title:"Trevas - Op\xe9rateurs temporels",authors:["hadrien"],tags:["Trevas"]},i=void 0,l={permalink:"/Trevas/fr/blog/trevas-temporal-operators",source:"@site/i18n/fr/docusaurus-plugin-content-blog/2024-06-21-temporal-operators.mdx",title:"Trevas - Op\xe9rateurs temporels",description:"Op\xe9rateurs temporels de Trevas",date:"2024-06-21T00:00:00.000Z",formattedDate:"21 juin 2024",tags:[{label:"Trevas",permalink:"/Trevas/fr/blog/tags/trevas"}],readingTime:3.14,hasTruncateMarker:!1,authors:[{name:"Hadrien Kohl",link:"https://github.com/hadrienk",title:"Hadrien Kohl Consulting - Developer",image:"/img/profile_pic_hadrien_kohl.jpg",key:"hadrien"}],frontMatter:{slug:"/trevas-temporal-operators",title:"Trevas - Op\xe9rateurs temporels",authors:["hadrien"],tags:["Trevas"]},prevItem:{title:"Trevas - SDMX",permalink:"/Trevas/fr/blog/trevas-sdmx"},nextItem:{title:"Trevas - Java 17",permalink:"/Trevas/fr/blog/trevas-java-17"}},s={authorsImageUrls:[void 0]},p=[{value:"Op\xe9rateurs temporels de Trevas",id:"op\xe9rateurs-temporels-de-trevas",level:3},{value:"Repr\xe9sentation Java",id:"repr\xe9sentation-java",level:4},{value:"Fonction <code>flow_to_stock</code>",id:"fonction-flow_to_stock",level:4},{value:"Fonction <code>stock_to_flow</code>",id:"fonction-stock_to_flow",level:4},{value:"Fonction <code>timeshift</code>",id:"fonction-timeshift",level:4}],u={toc:p};function d(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h3",{id:"op\xe9rateurs-temporels-de-trevas"},"Op\xe9rateurs temporels de Trevas"),(0,r.kt)("p",null,"La version 1.4.0 de Trevas introduit un support pr\xe9liminaire pour les types de date et d'heure\net les op\xe9rateurs."),(0,r.kt)("p",null,"La sp\xe9cification d\xe9crit les types temporels tels que ",(0,r.kt)("inlineCode",{parentName:"p"},"date"),", ",(0,r.kt)("inlineCode",{parentName:"p"},"time_period"),", ",(0,r.kt)("inlineCode",{parentName:"p"},"time")," et ",(0,r.kt)("inlineCode",{parentName:"p"},"duration"),". Cependant, les auteurs de Trevas trouvent\nces descriptions peu satisfaisantes. Cet article de blog d\xe9crit nos choix de mise en \u0153uvre et en quoi ils diff\xe8rent des sp\xe9cifications."),(0,r.kt)("p",null,"Dans la sp\xe9cification, ",(0,r.kt)("inlineCode",{parentName:"p"},"time_period")," (et les types ",(0,r.kt)("inlineCode",{parentName:"p"},"date"),") est d\xe9crit comme un type compos\xe9 avec un d\xe9but et une fin (ou un\nd\xe9but et une dur\xe9e). Cela complique la mise en \u0153uvre et apporte peu de valeur au langage car on peut simplement\nfonctionner directement sur une combinaison de dates ou de date et de dur\xe9e. Pour cette raison, nous avons d\xe9fini une alg\xe8bre entre les\ntypes temporels et n'avons pas encore impl\xe9ment\xe9 le type ",(0,r.kt)("inlineCode",{parentName:"p"},"time_period"),"."),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"r\xe9sultat (op\xe9rateurs)"),(0,r.kt)("th",{parentName:"tr",align:null},"date"),(0,r.kt)("th",{parentName:"tr",align:null},"dur\xe9e"),(0,r.kt)("th",{parentName:"tr",align:null},"nombre"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},(0,r.kt)("strong",{parentName:"td"},"date")),(0,r.kt)("td",{parentName:"tr",align:null},"n/a"),(0,r.kt)("td",{parentName:"tr",align:null},"date (+, -)"),(0,r.kt)("td",{parentName:"tr",align:null},"n/a")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},(0,r.kt)("strong",{parentName:"td"},"dur\xe9e")),(0,r.kt)("td",{parentName:"tr",align:null},"date (+, -)"),(0,r.kt)("td",{parentName:"tr",align:null},"dur\xe9e (+, -)"),(0,r.kt)("td",{parentName:"tr",align:null},"dur\xe9e (","*",")")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},(0,r.kt)("strong",{parentName:"td"},"num\xe9ro")),(0,r.kt)("td",{parentName:"tr",align:null},"n/a"),(0,r.kt)("td",{parentName:"tr",align:null},"dur\xe9e (","*",")"),(0,r.kt)("td",{parentName:"tr",align:null},"n/a")))),(0,r.kt)("p",null,"La fonction ",(0,r.kt)("inlineCode",{parentName:"p"},"period_indicator")," s'appuie sur la connaissance des p\xe9riodes pour les types qui ne sont pas suffisamment d\xe9finis pour le moment pour\n\xeatre impl\xe9ment\xe9s."),(0,r.kt)("h4",{id:"repr\xe9sentation-java"},"Repr\xe9sentation Java"),(0,r.kt)("p",null,"Le type VTL ",(0,r.kt)("inlineCode",{parentName:"p"},"date")," est repr\xe9sent\xe9 en interne comme le\ntype ",(0,r.kt)("a",{parentName:"p",href:"https://docs.oracle.com/en%2Fjava%2Fjavase%2F11%2Fdocs%2Fapi%2F%2F/java.base/java/time/Instant.html"},(0,r.kt)("inlineCode",{parentName:"a"},"java.time.Instant")),",\n",(0,r.kt)("a",{parentName:"p",href:"https://docs.oracle.com/en%2Fjava%2Fjavase%2F11%2Fdocs%2Fapi%2F%2F/java.base/java/time/ZonedDateTime.html"},(0,r.kt)("inlineCode",{parentName:"a"},"java.time.ZonedDateTime")),"\net ","[",(0,r.kt)("inlineCode",{parentName:"p"},"java.time.OffsetDateTime"),"]","(",(0,r.kt)("a",{parentName:"p",href:"https://docs.oracle.com/en%2Fjava%2Fjavase%2F11%2Fdocs%2Fapi%2F%2F/java.base/java/time/OffsetDateTime.html#"},"https://docs.oracle.com/en%2Fjava%2Fjavase%2F11%2Fdocs%2Fapi%2F%2F/java.base/java/time/OffsetDateTime.html#"),": ~:text=OffsetDateTime%20is%20an%20immutable%20representation,be%20stored%20in%20an%20OffsetDateTime%20.)"),(0,r.kt)("p",null,"Instant repr\xe9sente un moment pr\xe9cis dans le temps. Notez que ce type n'inclut pas les informations de fuseau horaire et est donc\nnon utilisable avec tous les op\xe9rateurs. On peut utiliser les types ",(0,r.kt)("inlineCode",{parentName:"p"},"ZonedDateTime")," et ",(0,r.kt)("inlineCode",{parentName:"p"},"OffsetDateTime")," lorsque la sauvegarde du fuseau horaire est n\xe9cessaire."),(0,r.kt)("p",null,"Le type de VTL ",(0,r.kt)("inlineCode",{parentName:"p"},"dur\xe9e")," est repr\xe9sent\xe9 en interne comme le\ntype ",(0,r.kt)("a",{parentName:"p",href:"https://www.troisten.org/troisten-extra/apidocs/org.troisten.extra/org/troisten/extra/PeriodDuration.html"},(0,r.kt)("inlineCode",{parentName:"a"},"org.troisten.extra.PeriodDuration")),"\ndu package ",(0,r.kt)("a",{parentName:"p",href:"https://www.troisten.org/troisten-extra/"},"threeten extra"),".\nIl repr\xe9sente une dur\xe9e utilisant \xe0 la fois des unit\xe9s calendaires (ann\xe9es, mois, jours) et une dur\xe9e temporelle (heures, minutes, secondes et nanosecondes)."),(0,r.kt)("h4",{id:"fonction-flow_to_stock"},"Fonction ",(0,r.kt)("inlineCode",{parentName:"h4"},"flow_to_stock")),(0,r.kt)("p",null,"La fonction ",(0,r.kt)("inlineCode",{parentName:"p"},"flow_to_stock")," convertit un ensemble de donn\xe9es avec interpr\xe9tation par flux en une interpr\xe9tation par stock. Cette transformation\nest utile lorsque vous souhaitez agr\xe9ger des donn\xe9es de flux (par exemple, taux de ventes ou de production) en donn\xe9es de stock cumul\xe9es (par exemple, inventaire total)."),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Syntaxe:")),(0,r.kt)("p",null,(0,r.kt)("inlineCode",{parentName:"p"},"r\xe9sultat:= flow_to_stock(op)")),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Param\xe8tres:")),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"op")," - L'ensemble de donn\xe9es d'entr\xe9e avec interpr\xe9tation du flux. L'ensemble de donn\xe9es doit avoir un identifiant de type ",(0,r.kt)("inlineCode",{parentName:"li"},"time"),", des identifiants additionels, et au moins une mesure de type ",(0,r.kt)("inlineCode",{parentName:"li"},"number"),".")),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"R\xe9sultat:")),(0,r.kt)("p",null,"La fonction renvoie un ensemble de donn\xe9es avec la m\xeame structure que celui en entr\xe9e, mais avec les valeurs converties en stock."),(0,r.kt)("h4",{id:"fonction-stock_to_flow"},"Fonction ",(0,r.kt)("inlineCode",{parentName:"h4"},"stock_to_flow")),(0,r.kt)("p",null,"La fonction ",(0,r.kt)("inlineCode",{parentName:"p"},"stock_to_flow")," convertit un ensemble de donn\xe9es avec interpr\xe9tation de stock en une interpr\xe9tation de flux. Ce\nLa transformation est utile lorsque vous souhaitez d\xe9river des donn\xe9es de flux \xe0 partir de donn\xe9es de stock cumul\xe9es."),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Syntaxe:")),(0,r.kt)("p",null,(0,r.kt)("inlineCode",{parentName:"p"},"r\xe9sultat:= stock_to_flow(op)")),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Param\xe8tres:")),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"op")," - L'ensemble de donn\xe9es d'entr\xe9e avec interpr\xe9tation par stock. L'ensemble de donn\xe9es doit avoir un identifiant de type ",(0,r.kt)("inlineCode",{parentName:"li"},"time"),", des identifiants additionels, et au moins une mesure de type ",(0,r.kt)("inlineCode",{parentName:"li"},"number"),".")),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"R\xe9sultat:")),(0,r.kt)("p",null,"La fonction renvoie un ensemble de donn\xe9es avec la m\xeame structure que celui en entr\xe9e, mais avec les valeurs converties en flux."),(0,r.kt)("h4",{id:"fonction-timeshift"},"Fonction ",(0,r.kt)("inlineCode",{parentName:"h4"},"timeshift")),(0,r.kt)("p",null,"La fonction ",(0,r.kt)("inlineCode",{parentName:"p"},"timeshift")," d\xe9cale la composante temporelle d'une plage de temps sp\xe9cifi\xe9e dans l'ensemble de donn\xe9es. Ceci est utile pour\nanalyser des donn\xe9es \xe0 diff\xe9rents d\xe9calages temporels, par exemple en comparant les valeurs actuelles aux valeurs pass\xe9es."),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Syntaxe:")),(0,r.kt)("p",null,(0,r.kt)("inlineCode",{parentName:"p"},"r\xe9sultat := timeshift(op, shiftNumber)")),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Param\xe8tres:")),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"op")," - Ensemble de donn\xe9es contenant des s\xe9ries chronologiques."),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"shiftNumber")," - Un entier repr\xe9sentant le nombre de p\xe9riodes \xe0 d\xe9caler. Les valeurs positives \xe9voluent dans le temps, tandis que\nles valeurs n\xe9gatives reculent.")),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"R\xe9sultat:")),(0,r.kt)("p",null,"La fonction renvoie un ensemble de donn\xe9es avec les identifiants temporels d\xe9cal\xe9s du nombre de p\xe9riodes sp\xe9cifi\xe9es."))}d.isMDXComponent=!0}}]);