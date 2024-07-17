 export class Dims {
    constructor(width, height, marginTop, marginRight, marginBottom, marginLeft) {
        this.width = width;
        this.height = height;
        this.marginTop = marginTop;
        this.marginRight = marginRight;
        this.marginBottom = marginBottom;
        this.marginLeft = marginLeft;
        this.innerWidth = width - marginLeft - marginRight;
        this.innerHeight = height - marginTop - marginBottom;
        this.containerTransform = `translate(${marginLeft}, ${marginTop})`;
        this.bottomAxisTransform = `translate(0, ${this.innerHeight})`;
        this.rightAxisTransform = `translate(${this.innerWidth}, 0)`;
    }
}