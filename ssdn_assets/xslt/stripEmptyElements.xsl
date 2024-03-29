<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    exclude-result-prefixes="xs"
    version="1.0">
<!-- This stylesheet removes empty elements and elements given the value of 'null' by OpenRefine. -->
    <xsl:output method="xml" version="1.0" byte-order-mark="no" encoding="UTF-8" indent="yes" />
    <xsl:template match="node()|@*">
        <xsl:if test="normalize-space(string(.)) != '' and normalize-space(string(.)) != 'null' and normalize-space(string(.)) != 'null null'">
            <xsl:copy>
                <xsl:apply-templates select="node()|@*" />
            </xsl:copy>
        </xsl:if>
    </xsl:template>
</xsl:stylesheet>
