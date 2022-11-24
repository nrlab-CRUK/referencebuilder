/*
    Need a .hg.conf file in the home directory to access UCSC databases.
    This file must be private to the user.

    See http://genomewiki.ucsc.edu/index.php/Genes_in_gtf_or_gff_format
*/
process createHgConf
{
    label 'tiny'
    tag 'home'

    publishDir "${System.getProperty('user.home')}", mode: 'copy'

    input:
        path homeConfFile

    output:
        path hgConfFile

    shell:
        hgConfFile = ".hg.conf"

        """
            echo "db.host=genome-mysql.cse.ucsc.edu" > !{hgConfFile}
            echo "db.user=genomep" >> !{hgConfFile}
            echo "db.password=password" >> !{hgConfFile}
            echo "central.db=hgcentral" >> !{hgConfFile}
            chmod 600 !{hgConfFile}
        """
}

workflow setupWF
{
    main:
        presentChoice = channel.fromPath("${System.getProperty('user.home')}/.hg.conf").branch
        {
            present: it.exists()
            needed: true
        }

        createHgConf(presentChoice.needed)

        hgConfChannel = presentChoice.present.mix(createHgConf.out)

    emit:
        hgConfChannel
}
